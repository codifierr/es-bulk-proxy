# Deployment Guide

This guide covers all deployment options for ES Bulk Proxy, from local development to production Kubernetes clusters.

## Table of Contents

- [Quick Start](#quick-start)
- [Docker Compose](#docker-compose)
- [Building from Source](#building-from-source)
- [Docker](#docker)
- [Kubernetes](#kubernetes)
- [Production Considerations](#production-considerations)

## Quick Start

The fastest way to get started with a complete stack including Elasticsearch, Prometheus, and Grafana:

```bash
cd deployments
docker compose up -d
```

This starts:
- **Elasticsearch** on port 9200
- **ES Proxy** on port 8080
- **Prometheus** on port 9090
- **Grafana** on port 3001 (admin/admin)

**Access Services:**
- Grafana Dashboard: http://localhost:3001/d/es-bulk-proxy-dashboard
- Prometheus: http://localhost:9090
- ES Proxy Metrics: http://localhost:8080/metrics
- Elasticsearch: http://localhost:9200

## Docker Compose

### Full Stack Deployment

The included `docker-compose.yml` provides a complete observability stack:

```bash
# Start all services
cd deployments
docker compose up -d

# View logs
docker compose logs -f es-bulk-proxy

# View all logs
docker compose logs -f

# Stop services
docker compose down

# Stop and remove volumes
docker compose down -v
```

### Custom Docker Compose

Create your own `docker-compose.yml`:

```yaml
version: '3.8'

services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
    ports:
      - "9200:9200"
    volumes:
      - es-data:/usr/share/elasticsearch/data

  es-bulk-proxy:
    image: ssingh3339/es-bulk-proxy:latest
    ports:
      - "8080:8080"
    environment:
      - ES_URL=http://elasticsearch:9200
      - FLUSH_INTERVAL=30s
      - MAX_BATCH_SIZE=10485760
      - MAX_BUFFER_SIZE=104857600
    depends_on:
      - elasticsearch
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 10s
      timeout: 5s
      retries: 3

volumes:
  es-data:
```

## Building from Source

### Prerequisites

- Go 1.26.2 or higher
- Make
- Git

### Build Steps

```bash
# Clone repository
git clone https://github.com/codifierr/es-bulk-proxy.git
cd es-bulk-proxy

# Download dependencies
make deps

# Build binary
make build

# Binary will be at ./es-bulk-proxy
```

### Run Locally

```bash
# Run with default config
./es-bulk-proxy

# Run with environment variables
ES_URL=http://localhost:9200 \
FLUSH_INTERVAL=10s \
./es-bulk-proxy

# Run in development mode
ENVIRONMENT=development ./es-bulk-proxy

# Run with config file
./es-bulk-proxy -config configs/config.yaml
```

### Development Commands

```bash
# Install pre-commit hooks
make precommit-install

# Format code
make format

# Run linters
make lint

# Run tests
make test

# Run integration tests (requires Docker)
make integration-test

# Start full dev environment
make dev

# Show all available commands
make help
```

## Docker

### Using Published Image

The easiest way to run with Docker:

```bash
docker run -d \
  --name es-bulk-proxy \
  -p 8080:8080 \
  -e ES_URL=http://elasticsearch:9200 \
  -e FLUSH_INTERVAL=30s \
  ssingh3339/es-bulk-proxy:latest
```

### Building Your Own Image

```bash
# Build image
docker build -t es-bulk-proxy:latest .

# Build for specific platform
docker build --platform linux/amd64 -t es-bulk-proxy:latest .

# Run your image
docker run -d \
  --name es-bulk-proxy \
  -p 8080:8080 \
  -e ES_URL=http://elasticsearch:9200 \
  es-bulk-proxy:latest
```

### Multi-Architecture Build

```bash
# Build for multiple architectures
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t myregistry/es-bulk-proxy:latest \
  --push .
```

### Docker Run Options

```bash
# With custom config file
docker run -d \
  --name es-bulk-proxy \
  -p 8080:8080 \
  -v $(pwd)/configs/config.yaml:/app/configs/config.yaml:ro \
  -e ES_URL=http://elasticsearch:9200 \
  ssingh3339/es-bulk-proxy:latest

# With resource limits
docker run -d \
  --name es-bulk-proxy \
  -p 8080:8080 \
  --memory="512m" \
  --cpus="1.0" \
  -e ES_URL=http://elasticsearch:9200 \
  ssingh3339/es-bulk-proxy:latest

# With healthcheck
docker run -d \
  --name es-bulk-proxy \
  -p 8080:8080 \
  --health-cmd="curl -f http://localhost:8080/health || exit 1" \
  --health-interval=10s \
  --health-timeout=5s \
  --health-retries=3 \
  -e ES_URL=http://elasticsearch:9200 \
  ssingh3339/es-bulk-proxy:latest
```

## Kubernetes

### Quick Deploy

```bash
# Deploy with default configuration
kubectl apply -f deployments/kubernetes.yaml

# Check deployment status
kubectl get pods -l app=es-bulk-proxy
kubectl get svc es-bulk-proxy

# View logs
kubectl logs -l app=es-bulk-proxy -f

# Port forward for testing
kubectl port-forward svc/es-bulk-proxy 8080:8080
```

### Kubernetes Manifest

The included `kubernetes.yaml` provides a production-ready deployment:

**Features:**
- Deployment with 2 replicas
- ClusterIP Service
- ConfigMap for configuration
- Horizontal Pod Autoscaler (HPA)
- PodDisruptionBudget (PDB)
- ServiceMonitor for Prometheus Operator
- Resource requests and limits
- Liveness and readiness probes

### Custom Kubernetes Deployment

Create your own manifests:

```yaml
# configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: es-bulk-proxy-config
  namespace: default
data:
  ES_URL: "http://elasticsearch:9200"
  FLUSH_INTERVAL: "30s"
  MAX_BATCH_SIZE: "10485760"
  MAX_BUFFER_SIZE: "104857600"
  RETRY_ATTEMPTS: "3"
---
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: es-bulk-proxy
  namespace: default
  labels:
    app: es-bulk-proxy
spec:
  replicas: 3
  selector:
    matchLabels:
      app: es-bulk-proxy
  template:
    metadata:
      labels:
        app: es-bulk-proxy
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      containers:
      - name: es-bulk-proxy
        image: ssingh3339/es-bulk-proxy:latest
        ports:
        - containerPort: 8080
          name: http
        envFrom:
        - configMapRef:
            name: es-bulk-proxy-config
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            cpu: 500m
            memory: 512Mi
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
---
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: es-bulk-proxy
  namespace: default
  labels:
    app: es-bulk-proxy
spec:
  selector:
    app: es-bulk-proxy
  ports:
  - port: 8080
    targetPort: 8080
    name: http
  type: ClusterIP
---
# hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: es-bulk-proxy
  namespace: default
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: es-bulk-proxy
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

### Helm Chart (Community)

For advanced Kubernetes deployments, consider creating a Helm chart with:

```bash
helm create es-bulk-proxy
```

Include in your chart:
- Configurable replicas and resources
- Optional Ingress configuration
- Service type options
- PodDisruptionBudget
- ServiceMonitor for Prometheus
- Network policies

### Kubernetes Operations

```bash
# Scale deployment
kubectl scale deployment es-bulk-proxy --replicas=5

# Update configuration
kubectl edit configmap es-bulk-proxy-config
kubectl rollout restart deployment es-bulk-proxy

# View metrics from pod
kubectl port-forward deployment/es-bulk-proxy 8080:8080
curl http://localhost:8080/metrics

# Check HPA status
kubectl get hpa es-bulk-proxy

# Check PDB status
kubectl get pdb es-bulk-proxy

# View events
kubectl get events --sort-by='.lastTimestamp' | grep es-bulk-proxy

# Delete deployment
kubectl delete -f deployments/kubernetes.yaml
```

## Production Considerations

### High Availability

1. **Multiple Replicas**: Run at least 2-3 replicas
   ```bash
   kubectl scale deployment es-bulk-proxy --replicas=3
   ```

2. **Pod Disruption Budget**: Ensure minimum availability
   ```yaml
   apiVersion: policy/v1
   kind: PodDisruptionBudget
   metadata:
     name: es-bulk-proxy
   spec:
     minAvailable: 1
     selector:
       matchLabels:
         app: es-bulk-proxy
   ```

3. **Anti-Affinity**: Spread pods across nodes
   ```yaml
   affinity:
     podAntiAffinity:
       preferredDuringSchedulingIgnoredDuringExecution:
       - weight: 100
         podAffinityTerm:
           labelSelector:
             matchLabels:
               app: es-bulk-proxy
           topologyKey: kubernetes.io/hostname
   ```

### Resource Management

1. **Set Resource Requests and Limits**:
   ```yaml
   resources:
     requests:
       cpu: 200m
       memory: 256Mi
     limits:
       cpu: 1000m
       memory: 1Gi
   ```

2. **Tune Buffer Sizes**: Match to memory limits
   ```bash
   # For 512Mi memory limit
   export MAX_BUFFER_SIZE=104857600  # 100MB
   ```

### Security

1. **Use Non-Root User** (already in Dockerfile):
   ```dockerfile
   USER nonroot:nonroot
   ```

2. **Read-Only Root Filesystem**:
   ```yaml
   securityContext:
     readOnlyRootFilesystem: true
     runAsNonRoot: true
   ```

3. **Network Policies**:
   ```yaml
   apiVersion: networking.k8s.io/v1
   kind: NetworkPolicy
   metadata:
     name: es-bulk-proxy
   spec:
     podSelector:
       matchLabels:
         app: es-bulk-proxy
     policyTypes:
     - Ingress
     - Egress
     ingress:
     - from:
       - podSelector: {}
       ports:
       - protocol: TCP
         port: 8080
     egress:
     - to:
       - podSelector:
           matchLabels:
             app: elasticsearch
       ports:
       - protocol: TCP
         port: 9200
   ```

### Monitoring

1. **Prometheus ServiceMonitor**:
   ```yaml
   apiVersion: monitoring.coreos.com/v1
   kind: ServiceMonitor
   metadata:
     name: es-bulk-proxy
   spec:
     selector:
       matchLabels:
         app: es-bulk-proxy
     endpoints:
     - port: http
       path: /metrics
       interval: 30s
   ```

2. **Grafana Dashboard**: Import `deployments/grafana-dashboard.json`

3. **Alerts**: Set up Prometheus alerts
   ```yaml
   - alert: ESProxyHighBufferUsage
     expr: es_proxy_buffer_size_bytes > 40000000
     for: 5m
     annotations:
       summary: "ES Proxy buffer usage is high"
   ```

### Load Balancing

1. **Kubernetes Service** (default - load balances automatically)
2. **Ingress** for external access:
   ```yaml
   apiVersion: networking.k8s.io/v1
   kind: Ingress
   metadata:
     name: es-bulk-proxy
   spec:
     rules:
     - host: es-proxy.example.com
       http:
         paths:
         - path: /
           pathType: Prefix
           backend:
             service:
               name: es-bulk-proxy
               port:
                 number: 8080
   ```

### Logging

1. **Centralized Logging**: Send logs to aggregator
   ```bash
   kubectl logs -l app=es-bulk-proxy -f | fluentd
   ```

2. **Log Level**: Use production mode
   ```yaml
   env:
   - name: ENVIRONMENT
     value: "production"  # JSON logs
   ```

### Backup and Disaster Recovery

1. **Stateless Design**: No data loss on pod restart
2. **Buffer Recovery**: Automatically retries failed batches
3. **Health Checks**: Kubernetes auto-restarts unhealthy pods

### Performance Tuning

1. **Horizontal Scaling**: Use HPA based on CPU/memory
2. **Vertical Scaling**: Increase resources if needed
3. **Buffer Tuning**: See [CONFIGURATION.md](CONFIGURATION.md)

### Updates and Rollbacks

```bash
# Rolling update
kubectl set image deployment/es-bulk-proxy \
  es-bulk-proxy=ssingh3339/es-bulk-proxy:v1.2.0

# Check rollout status
kubectl rollout status deployment/es-bulk-proxy

# Rollback if needed
kubectl rollout undo deployment/es-bulk-proxy

# View rollout history
kubectl rollout history deployment/es-bulk-proxy
```

## Troubleshooting Deployments

### Docker Issues

```bash
# Check container logs
docker logs es-bulk-proxy

# Check container health
docker inspect es-bulk-proxy | grep -A 10 Health

# Restart container
docker restart es-bulk-proxy

# Check resource usage
docker stats es-bulk-proxy
```

### Kubernetes Issues

```bash
# Pod not starting
kubectl describe pod -l app=es-bulk-proxy
kubectl logs -l app=es-bulk-proxy --previous

# Service not reachable
kubectl get endpoints es-bulk-proxy
kubectl port-forward svc/es-bulk-proxy 8080:8080

# High memory usage
kubectl top pods -l app=es-bulk-proxy

# Check HPA
kubectl describe hpa es-bulk-proxy
```

## Next Steps

- Configure advanced settings: [CONFIGURATION.md](CONFIGURATION.md)
- Set up monitoring: [GRAFANA_DASHBOARD.md](deployments/GRAFANA_DASHBOARD.md)
- Integrate with your application: [README.md](README.md)
