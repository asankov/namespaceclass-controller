package controller

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	inventoryAnnotation = "namespaceclass.akuity.io/inventory"
	managedByAnnotation = "namespaceclass.akuity.io/managed-by"
)

type managedResource struct {
	UID      types.UID                   `json:"uid,omitempty"`
	Name     string                      `json:"name"`
	Resource schema.GroupVersionResource `json:"resource"`
}

type inventory map[string]managedResource

func resourceKey(resource schema.GroupVersionResource, name string) string {
	return fmt.Sprintf("%s/%s/%s", resource.Group, resource.Resource, name)
}

func readInventory(namespace *corev1.Namespace) (inventory, error) {
	records := make(inventory)

	data := namespace.Annotations[inventoryAnnotation]
	if data == "" {
		return records, nil
	}

	if err := json.Unmarshal([]byte(data), &records); err != nil {
		return nil, fmt.Errorf("error while unmarshalling inventory: %w", err)
	}

	if records == nil {
		records = make(inventory)
	}

	return records, nil
}

func (r *NamespaceReconciler) saveInventory(ctx context.Context, namespace *corev1.Namespace, inventory inventory) error {
	data, err := json.Marshal(inventory)
	if err != nil {
		return fmt.Errorf("error while encoding inventory: %w", err)
	}

	if namespace.Annotations[inventoryAnnotation] == string(data) {
		return nil
	}

	before := namespace.DeepCopy()
	if namespace.Annotations == nil {
		namespace.Annotations = make(map[string]string)
	}
	namespace.Annotations[inventoryAnnotation] = string(data)

	if err := r.Patch(ctx, namespace, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("save managed resource inventory: %w", err)
	}

	return nil
}
