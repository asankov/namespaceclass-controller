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
	"time"

	namespaceclassv1alpha1 "github.com/asankov/namespaceclass/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	fieldManager = "namespaceclass-controller"

	requeueDuration = 5 * time.Second
)

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
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
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

	var preparedResources []preparedResource

	namespaceClassName := namespace.Labels["namespaceclass.akuity.io/name"]

	// if namespace has a namespace class - read it and prepare all resources for creation
	if namespaceClassName != "" {
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

		preparedResources, err = r.prepareResources(&namespace, objects)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	// we call syncResources regardless or not of whether namespace has a namespace class
	// 	- for namespaces that never had namespace class - this call will be a no-op
	// 	- for namespaces that had changed namespace classes - this will replace resources
	// 	- for namespaces whose namespace class changes - this call will apply the new resources and delete the stale ones
	hasPendingDeletion, err := r.syncResources(ctx, &namespace, preparedResources)
	if err != nil {
		return ctrl.Result{}, err
	}

	if hasPendingDeletion {
		log.Info("Applied resources; waiting for pending deletions", "namespace", req.Name, "requeueAfter", requeueDuration.String())

		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	log.Info("Applied resources successfully", "namespace", req.Name)

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

// validateResources validates that the resources can be created by the controller.
//
// NOTE from the author: initially I created this function to verify we only create NetworkPolicy resources
// because this is what the controller has permissions for.
// Eventually, I decided to not use it, since the assignment mentioned creating resources of ANY kind.
// For that to be possible we need to either:
//   - give the controller RBAC permissions for `*:*` (not a good idea)
//   - document the limitation and expect that the user of this controller will provide a ServiceAccount with proper permissions to the controller.
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

		annotations := object.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
		}
		annotations[managedByAnnotation] = fieldManager
		object.SetAnnotations(annotations)

		// object in the NamespaceClass should not have an owner since it will be owned by the namespace.
		if len(object.GetOwnerReferences()) != 0 {
			return nil, fmt.Errorf(
				"resources[%d]: metadata.ownerReferences must be omitted", i,
			)
		}

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

func (r *NamespaceReconciler) syncResources(ctx context.Context, namespace *corev1.Namespace, resources []preparedResource) (hasPendingDeletion bool, err error) {
	inventory, err := readInventory(namespace)
	if err != nil {
		return false, err
	}

	if len(resources) == 0 && len(inventory) == 0 {
		return false, nil
	}

	desired := make(map[string]struct{}, len(resources))

	for _, resource := range resources {
		key := resourceKey(resource.Resource, resource.Object.GetName())
		desired[key] = struct{}{}

		entry, ok := inventory[key]
		if ok {
			// Preserve the recorded UID and just refresh the API version.
			entry.Resource = resource.Resource
		} else {
			entry = managedResource{
				Resource: resource.Resource,
				Name:     resource.Object.GetName(),
				// UID is empty here, which means the record should be created
			}
		}
		inventory[key] = entry
	}

	if err := r.saveInventory(ctx, namespace, inventory); err != nil {
		return false, err
	}

	if err := r.applyDesiredResources(ctx, namespace, resources, inventory); err != nil {
		return false, err
	}

	hasPendingDeletion, err = r.deleteObsoleteResources(ctx, namespace, desired, inventory)
	if err != nil {
		return false, err
	}

	if err := r.saveInventory(ctx, namespace, inventory); err != nil {
		return false, err
	}

	return hasPendingDeletion, nil
}

func (r *NamespaceReconciler) applyDesiredResources(ctx context.Context, namespace *corev1.Namespace, resources []preparedResource, inventory inventory) error {
	for _, resource := range resources {
		key := resourceKey(resource.Resource, resource.Object.GetName())
		entry := inventory[key]
		applied, err := r.applyResource(ctx, namespace, resource, entry.UID)
		if err != nil {
			return err
		}

		entry.UID = applied.GetUID()
		inventory[key] = entry

		if err := r.saveInventory(ctx, namespace, inventory); err != nil {
			return err
		}
	}
	return nil
}

func (r *NamespaceReconciler) deleteObsoleteResources(ctx context.Context, namespace *corev1.Namespace, desired map[string]struct{}, inventory inventory) (hasPendingDeletion bool, err error) {
	hasPendingDeletion = false

	for key, entry := range inventory {
		if _, wanted := desired[key]; wanted {
			continue
		}

		api := r.Dynamic.Resource(entry.Resource).Namespace(namespace.Name)
		current, err := api.Get(ctx, entry.Name, v1.GetOptions{})

		if apierrors.IsNotFound(err) {
			delete(inventory, key)
			continue
		}
		if err != nil {
			return false, fmt.Errorf("error while getting obsolete resource %q: %w", entry.Name, err)
		}

		// A replaced or unrelated object must never be deleted.
		if !ownsResource(namespace, current) ||
			(entry.UID != "" && current.GetUID() != entry.UID) {
			logf.FromContext(ctx).Info("Stopped tracking resource with different identity or ownership", "resource", entry.Resource.String(), "name", entry.Name)
			delete(inventory, key)
			continue
		}

		if current.GetDeletionTimestamp().IsZero() {
			uid := current.GetUID()
			resourceVersion := current.GetResourceVersion()

			err := api.Delete(ctx, entry.Name, v1.DeleteOptions{Preconditions: &v1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}})
			if apierrors.IsNotFound(err) {
				delete(inventory, key)
				continue
			}
			if err != nil {
				return false, fmt.Errorf("error while deleting obsolete resource %q: %w", entry.Name, err)
			}
		}

		// if we are here it means either:
		// - we requested deletion for that object
		// - deletion is already requested (current.GetDeletionTimestamp().IsZero() == false) but it has not completed
		hasPendingDeletion = true
	}

	return hasPendingDeletion, nil
}

func (r *NamespaceReconciler) applyResource(ctx context.Context, namespace *corev1.Namespace, resource preparedResource, expectedUID types.UID) (*unstructured.Unstructured, error) {
	object := resource.Object.DeepCopy()
	api := r.Dynamic.Resource(resource.Resource).Namespace(namespace.Name)

	existing, err := api.Get(ctx, object.GetName(), v1.GetOptions{})
	if apierrors.IsNotFound(err) {
		created, err := api.Create(ctx, object, v1.CreateOptions{FieldManager: fieldManager})
		if err != nil {
			return nil, fmt.Errorf("error while creating %s %q: %w", object.GetKind(), object.GetName(), err)
		}
		return created, nil
	}

	if err != nil {
		if apierrors.IsForbidden(err) {
			return nil, fmt.Errorf(
				"controller lacks permission to read %s %q in namespace %q; grant its ServiceAccount 'get' access to resource %q in API group %q",
				object.GetKind(),
				object.GetName(),
				namespace.Name,
				resource.Resource.Resource,
				resource.Resource.Group,
			)
		}
		return nil, fmt.Errorf("error while getting %s %q: %w", object.GetKind(), object.GetName(), err)
	}

	if expectedUID != "" && existing.GetUID() != expectedUID {
		return nil, fmt.Errorf("managed resource %s %q was replaced", object.GetKind(), object.GetName())
	}

	if !ownsResource(namespace, existing) {
		return nil, fmt.Errorf("resource %s %q is not owned by Namespace %q", object.GetKind(), object.GetName(), namespace.Name)
	}

	// this would make the apply fail if someone else already modified the object
	object.SetResourceVersion(existing.GetResourceVersion())

	data, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("error while encoding %s %q: %w", object.GetKind(), object.GetName(), err)
	}

	patched, err := api.Patch(ctx, object.GetName(), types.ApplyPatchType, data, v1.PatchOptions{FieldManager: fieldManager})
	if err != nil {
		return nil, fmt.Errorf(
			"error while applying %s %q: %w", object.GetKind(), object.GetName(), err,
		)
	}

	return patched, nil
}

func ownsResource(namespace *corev1.Namespace, object *unstructured.Unstructured) bool {
	owner := v1.GetControllerOf(object)

	return object.GetAnnotations()[managedByAnnotation] == fieldManager &&
		owner != nil &&
		owner.APIVersion == "v1" &&
		owner.Kind == "Namespace" &&
		owner.Name == namespace.Name &&
		owner.UID == namespace.UID
}

// SetupWithManager sets up the controller with the Manager.
func (r *NamespaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Namespace{}, "namespaceclass.name", namespaceClassIndexValues); err != nil {
		return fmt.Errorf("error while indexing Namespaces by NamespaceClass: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Namespace{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Watches(&namespaceclassv1alpha1.NamespaceClass{}, handler.EnqueueRequestsFromMapFunc(r.namespacesForNamespaceClass)).
		Named("namespace").
		Complete(r)
}

func (r *NamespaceReconciler) namespacesForNamespaceClass(ctx context.Context, namespaceClass client.Object) []reconcile.Request {
	var namespaces corev1.NamespaceList
	if err := r.List(ctx, &namespaces, client.MatchingFields{"namespaceclass.name": namespaceClass.GetName()}); err != nil {
		logf.FromContext(ctx).Error(err, "error while listing Namespaces for NamespaceClass", "namespaceClass", namespaceClass.GetName())
		return nil
	}

	requests := make([]reconcile.Request, 0, len(namespaces.Items))
	for _, namespace := range namespaces.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKey{Name: namespace.Name}})
	}
	return requests
}

func namespaceClassIndexValues(object client.Object) []string {
	if name := object.GetLabels()["namespaceclass.akuity.io/name"]; name != "" {
		return []string{name}
	}
	return nil
}
