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
	"fmt"

	namespaceclassv1alpha1 "github.com/asankov/namespaceclass/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// NamespaceReconciler reconciles a Namespace object
type NamespaceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=namespaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=namespaces/finalizers,verbs=update
// +kubebuilder:rbac:groups=namespaceclass.akuity.io,resources=namespaceclasses,verbs=get;list;watch

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

	log.Info(fmt.Sprintf("Namspace [%s] has NamespaceClass [%s]", req.Name, namespaceClass.Name))

	// TODO(asankov): apply resources
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NamespaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Namespace{}).
		Named("namespace").
		Complete(r)
}
