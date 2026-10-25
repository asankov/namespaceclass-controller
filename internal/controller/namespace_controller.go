/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"

	namespaceclassv1alpha1 "github.com/asankov/namespaceclass/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const fieldManager = "namespaceclass-controller"

// NamespaceReconciler reconciles a Namespace object
type NamespaceReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Mapper  meta.RESTMapper
	Dynamic dynamic.Interface
}

// preparedResource is a resource that is ready to be applied to the API server.
type preparedResource struct {
	Object   *unstructured.Unstructured
	Resource schema.GroupVersionResource
}

// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=namespaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=namespaces/finalizers,verbs=update
// +kubebuilder:rbac:groups=namespaceclass.akuity.io,resources=namespaceclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Namespace object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/reconcile
func (r *NamespaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var namespace corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: req.Name}, &namespace); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !namespace.DeletionTimestamp.IsZero() {
		// ignore namespace being deleted
		return ctrl.Result{}, nil
	}

	namespaceClassName, ok := namespace.Labels["namespaceclass.akuity.io/name"]
	if !ok {
		// namespace has no namespace class, skip
		return ctrl.Result{}, nil
	}

	var namespaceClass namespaceclassv1alpha1.NamespaceClass
	if err := r.Get(ctx, client.ObjectKey{Name: namespaceClassName}, &namespaceClass); err != nil {
		if apierrors.IsNotFound(err) {
			// log an error, but do not fail the reconcile, because it might get stuck in a loop
			log.Info("referenced namespace class not found", "namespaceClass", namespaceClassName)
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, fmt.Errorf("error while getting NamespaceClass: %w", err)
	}

	log.Info(fmt.Sprintf("Namespace [%s] has NamespaceClass [%s]", req.Name, namespaceClass.Name))

	objects, err := decodeResources(namespaceClass.Spec.Resources)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf(
			"error while validating NamespaceClass %q: %w", namespaceClassName, err,
		)
	}

	if err := validateResources(objects); err != nil {
		return ctrl.Result{}, fmt.Errorf("error while validating resources: %w", err)
	}

	preparedResources, err := r.prepareResources(&namespace, objects)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.applyResources(ctx, &namespace, preparedResources); err != nil {
		return ctrl.Result{}, err
	}

	log.Info(fmt.Sprintf("All resources applied succesfully to namespace [%s]", req.Name))

	return ctrl.Result{}, nil
}

func decodeResources(resources []runtime.RawExtension) ([]unstructured.Unstructured, error) {
	objects := make([]unstructured.Unstructured, 0, len(resources))

	type resourceIdentity struct {
		Group string
		Kind  string
		Name  string
	}
	seen := make(map[resourceIdentity]struct{}, len(resources))

	for i, resource := range resources {
		// Marshal RawExtension to support both its Raw and Object forms.
		data, err := json.Marshal(resource)
		if err != nil {
			return nil, fmt.Errorf("resources[%d]: encode manifest: %w", i, err)
		}

		var object unstructured.Unstructured
		if err := json.Unmarshal(data, &object.Object); err != nil {
			return nil, fmt.Errorf("resources[%d]: decode manifest: %w", i, err)
		}

		if object.Object == nil {
			return nil, fmt.Errorf("resources[%d]: manifest must be an object", i)
		}

		if object.GetAPIVersion() == "" || object.GetKind() == "" {
			return nil, fmt.Errorf(
				"resources[%d]: apiVersion and kind are required", i,
			)
		}

		groupVersion, err := schema.ParseGroupVersion(object.GetAPIVersion())
		if err != nil {
			return nil, fmt.Errorf("resources[%d]: invalid apiVersion: %w", i, err)
		}

		if object.GetName() == "" {
			return nil, fmt.Errorf("resources[%d]: metadata.name is required", i)
		}

		if object.GetNamespace() != "" {
			return nil, fmt.Errorf("resources[%d]: metadata.namespace should not be set", i)
		}

		identity := resourceIdentity{
			Group: groupVersion.Group,
			Kind:  object.GetKind(),
			Name:  object.GetName(),
		}
		if _, exists := seen[identity]; exists {
			return nil, fmt.Errorf(
				"resources[%d]: duplicate resource %s %q",
				i, object.GroupVersionKind().GroupKind(), object.GetName(),
			)
		}
		seen[identity] = struct{}{}

		objects = append(objects, object)
	}

	return objects, nil
}

func validateResources(resources []unstructured.Unstructured) error {
	for _, r := range resources {
		// for now, we only allow resources of type NetwotkPolicy to be created
		// the CRD spec allows all kinds, so this check can be relaxed in the future.
		if gvk := r.GroupVersionKind(); gvk.Group != "networking.k8s.io" || gvk.Kind != "NetworkPolicy" {
			return fmt.Errorf("resource %s %s: only resource of type NetworkPolicy can be created via the NamespaceClass", gvk.Kind, r.GetName())
		}
	}

	return nil
}

func (r *NamespaceReconciler) prepareResources(namespace *corev1.Namespace, objects []unstructured.Unstructured) ([]preparedResource, error) {
	preparedResources := make([]preparedResource, 0, len(objects))

	for i := range objects {
		object := objects[i].DeepCopy()
		gvk := object.GroupVersionKind()

		mapping, err := r.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, fmt.Errorf("resolve %s %q: %w", gvk, object.GetName(), err)
		}

		if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
			// only allow resources which are scoped to a namespace
			return nil, fmt.Errorf("%s %q is cluster-scoped", gvk, object.GetName())
		}

		object.SetNamespace(namespace.Name)

		if err := controllerutil.SetControllerReference(namespace, object, r.Scheme); err != nil {
			return nil, fmt.Errorf("error while setting controller reference to %s %q: %w", gvk, object.GetName(), err)
		}

		preparedResources = append(preparedResources, preparedResource{
			Object:   object,
			Resource: mapping.Resource,
		})
	}
	return preparedResources, nil
}

func (r *NamespaceReconciler) applyResources(ctx context.Context, namespace *corev1.Namespace, resources []preparedResource) error {
	for _, resource := range resources {
		if err := r.applyResource(ctx, namespace, resource); err != nil {
			return err
		}
	}

	return nil
}

func (r *NamespaceReconciler) applyResource(ctx context.Context, namespace *corev1.Namespace, resource preparedResource) error {
	object := resource.Object.DeepCopy()
	api := r.Dynamic.Resource(resource.Resource).Namespace(namespace.Name)

	existing, err := api.Get(ctx, object.GetName(), v1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := api.Create(ctx, object, v1.CreateOptions{FieldManager: fieldManager}); err != nil {
			return fmt.Errorf("error while creating %s %q: %w", object.GetKind(), object.GetName(), err)
		}
		return nil
	}

	if err != nil {
		return fmt.Errorf("error while getting %s %q: %w", object.GetKind(), object.GetName(), err)
	}

	owner := v1.GetControllerOf(existing)
	if owner == nil || owner.APIVersion != "v1" || owner.Kind != "Namespace" || owner.Name != namespace.Name || owner.UID != namespace.UID {
		return fmt.Errorf("resource %s %q is not owned by Namespace %q", object.GetKind(), object.GetName(), namespace.Name)
	}

	// this would make the apply fail if someone else already modified the object
	object.SetResourceVersion(existing.GetResourceVersion())

	data, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("error while encoding %s %q: %w", object.GetKind(), object.GetName(), err)
	}

	if _, err := api.Patch(ctx, object.GetName(), types.ApplyPatchType, data, v1.PatchOptions{FieldManager: fieldManager}); err != nil {
		return fmt.Errorf(
			"apply %s %q: %w", object.GetKind(), object.GetName(), err,
		)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NamespaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Namespace{}).
		Named("namespace").
		Complete(r)
}
