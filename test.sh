# Provision the cluster and select it.
make setup-test-e2e

# Generate and install the CRD and RBAC manifests.
make manifests generate
make install

# Build the controller image and load it into kind.
make docker-build IMG=namespaceclass:dev
kind load docker-image namespaceclass:dev --name namespaceclass-test-e2e

# Deploy the controller and wait for readiness.
make deploy IMG=namespaceclass:dev
kubectl rollout status deployment/namespaceclass-controller-manager -n namespaceclass-system --timeout=120s

# Create a NamespaceClass
kubectl apply -f - <<'EOF'
apiVersion: namespaceclass.akuity.io/v1alpha1
kind: NamespaceClass
metadata:
  name: example
spec:
  resources:
    - apiVersion: networking.k8s.io/v1
      kind: NetworkPolicy
      metadata:
        name: default-deny-ingress
      spec:
        podSelector: {}
        policyTypes:
          - Ingress
        ingress: []
EOF

# Create a Namespace that uses the new NamespaceClass
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: example-namespace
  labels:
    namespaceclass.akuity.io/name: example
EOF

# Add a new resource to the NamespaceClass to test that it is picked up and created in the example-namespace ns
kubectl apply -f - <<'EOF'
apiVersion: namespaceclass.akuity.io/v1alpha1
kind: NamespaceClass
metadata:
  name: example
spec:
  resources:
    - apiVersion: networking.k8s.io/v1
      kind: NetworkPolicy
      metadata:
        name: default-deny-ingress
      spec:
        podSelector: {}
        policyTypes:
          - Ingress
        ingress: []

    - apiVersion: networking.k8s.io/v1
      kind: NetworkPolicy
      metadata:
        name: allow-trusted-ingress
      spec:
        podSelector: {}
        policyTypes:
          - Ingress
        ingress:
          - from:
              - namespaceSelector:
                  matchLabels:
                    access: trusted
EOF

# Delete a resource from the NamespaceClass to test that it is delete from the example-namespace ns
kubectl apply -f - <<'EOF'
apiVersion: namespaceclass.akuity.io/v1alpha1
kind: NamespaceClass
metadata:
  name: example
spec:
  resources:
    - apiVersion: networking.k8s.io/v1
      kind: NetworkPolicy
      metadata:
        name: default-deny-ingress
      spec:
        podSelector: {}
        policyTypes:
          - Ingress
        ingress: []
EOF

# Create a new NamespaceClass
kubectl apply -f - <<'EOF'
apiVersion: namespaceclass.akuity.io/v1alpha1
kind: NamespaceClass
metadata:
  name: new-namespace-class
spec:
  resources:
    - apiVersion: networking.k8s.io/v1
      kind: NetworkPolicy
      metadata:
        name: new-namespace-class-policy
      spec:
        podSelector: {}
        policyTypes:
          - Ingress
        ingress: []
EOF

# Assign new NamespaceClass to the example-namespace ns to test that old resources are deleted and new ones are created
kubectl label namespace example-namespace namespaceclass.akuity.io/name=new-namespace-class --overwrite
