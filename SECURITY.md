# Security Policy

## Supported Versions

We release security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

We take the security of ES Bulk Proxy seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### How to Report

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them via one of the following methods:

1. **GitHub Security Advisories** (Preferred)
   - Navigate to the [Security tab](../../security) of this repository
   - Click "Report a vulnerability"
   - Fill out the advisory form with details

2. **Email** (Alternative)
   - Send an email to the project maintainers
   - Include "SECURITY" in the subject line
   - Provide detailed information about the vulnerability

### What to Include

Please include as much of the following information as possible:

- **Type of issue** (e.g., buffer overflow, injection, authentication bypass)
- **Full paths of source file(s)** related to the vulnerability
- **Location of the affected source code** (tag/branch/commit or direct URL)
- **Step-by-step instructions** to reproduce the issue
- **Proof-of-concept or exploit code** (if possible)
- **Impact of the issue**, including how an attacker might exploit it

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your report within 48 hours
- **Initial Assessment**: We will perform an initial assessment within 5 business days
- **Status Updates**: We will keep you informed of our progress
- **Resolution**: We aim to resolve critical issues within 30 days
- **Credit**: With your permission, we will credit you in our security advisory

## Security Best Practices

### Deployment Recommendations

When deploying ES Bulk Proxy in production, follow these security best practices:

#### 1. Network Security

- **Internal Network Only**: Deploy the proxy in a private network, not exposed to the public internet
- **Firewall Rules**: Restrict access to only authorized clients (e.g., Zenarmor instances)
- **TLS/SSL**: Use HTTPS for all communications, especially if crossing network boundaries
- **Network Segmentation**: Isolate the proxy and Elasticsearch in a dedicated security zone

```yaml
# Example: Restrict access in Kubernetes
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: es-proxy-policy
spec:
  podSelector:
    matchLabels:
      app: es-proxy
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: authorized-client
```

#### 2. Authentication & Authorization

- **Elasticsearch Security**: Enable Elasticsearch security features (authentication, TLS)
- **API Keys**: Use API keys or tokens for Elasticsearch authentication
- **Least Privilege**: Configure minimum required permissions for the proxy's Elasticsearch user

```yaml
# Example: Use environment variables for credentials
environment:
  ES_URL: "https://elasticsearch:9200"
  ES_USERNAME: ${ES_USERNAME}
  ES_PASSWORD: ${ES_PASSWORD}
```

#### 3. Resource Limits

- **Memory Limits**: Set appropriate memory limits to prevent OOM conditions
- **CPU Limits**: Configure CPU limits to prevent resource exhaustion
- **Buffer Size**: Configure max buffer size to prevent memory exhaustion attacks

```yaml
# Example: Kubernetes resource limits
resources:
  limits:
    memory: "512Mi"
    cpu: "500m"
  requests:
    memory: "256Mi"
    cpu: "250m"
```

#### 4. Input Validation

The proxy implements several input validation mechanisms:

- **Content-Type Validation**: Only accepts `application/x-ndjson` for /_bulk
- **Size Limits**: Enforces configurable maximum buffer sizes
- **Backpressure**: Returns HTTP 429 when buffer is full
- **Request Validation**: Validates bulk request format before buffering

#### 5. Monitoring & Logging

- **Enable Audit Logs**: Monitor all requests, especially failed ones
- **Metrics Monitoring**: Set up alerts for unusual patterns
  - High error rates
  - Excessive buffer growth
  - Unusual request patterns
- **Log Rotation**: Configure log rotation to prevent disk exhaustion

```yaml
# Example: Configure logging level
server:
  log_level: "info"  # Use "debug" only for troubleshooting
```

#### 6. Container Security

- **Non-Root User**: The Docker image runs as a non-root user (UID 1000)
- **Read-Only Filesystem**: Mount the root filesystem as read-only where possible
- **Scan Images**: Regularly scan container images for vulnerabilities
- **Minimal Image**: Uses scratch-based image with no shell or package manager

```dockerfile
# Security features in our Dockerfile
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
USER 65534:65534
```

#### 7. Configuration Security

- **Secrets Management**: Never hardcode credentials in configuration files
- **Environment Variables**: Use environment variables or secret managers for sensitive data
- **File Permissions**: Restrict access to configuration files (chmod 600)

```bash
# Example: Using Docker secrets
docker service create \
  --secret es_password \
  --env ES_PASSWORD_FILE=/run/secrets/es_password \
  es-bulk-proxy
```

### Kubernetes Specific

```yaml
# Example: Security context in Kubernetes
securityContext:
  runAsNonRoot: true
  runAsUser: 65534
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false
  capabilities:
    drop:
      - ALL
```

## Known Security Considerations

### 1. In-Memory Buffering

- **Data Persistence**: Buffered data is stored in RAM only and will be lost on crashes
- **Memory Exhaustion**: Large bursts can cause high memory usage
- **Mitigation**: Configure appropriate buffer limits and monitoring

### 2. Immediate Response Mode

- **Default Behavior**: Returns success immediately without waiting for Elasticsearch confirmation
- **Risk**: Clients may assume data is persisted when it's still in buffer
- **Mitigation**: Ensure proper monitoring and alerting on buffer flush failures

### 3. Retry Logic

- **Exponential Backoff**: Failed bulk requests are retried with exponential backoff
- **Risk**: Extended Elasticsearch outages can cause buffer overflow
- **Mitigation**: Monitor buffer size and configure appropriate limits

### 4. Request Classification

- **Pattern Matching**: Uses URL path patterns to classify requests
- **Risk**: Malformed or unusual requests might bypass classification
- **Mitigation**: All non-bulk requests are transparently proxied

## Security Features

### Built-in Protections

- ✅ **Buffer Overflow Protection**: Returns HTTP 429 when buffer is full
- ✅ **Request Size Limits**: Configurable maximum buffer size
- ✅ **Non-Root Execution**: Runs as non-privileged user
- ✅ **Graceful Shutdown**: Flushes buffers before terminating
- ✅ **Health Checks**: Provides readiness and liveness endpoints
- ✅ **Structured Logging**: JSON logs for security auditing
- ✅ **Metrics Export**: Prometheus metrics for monitoring

### Dependencies

We regularly update dependencies to patch known vulnerabilities:

```bash
# Check for dependency vulnerabilities
go list -json -m all | nancy sleuth

# Update dependencies
go get -u ./...
go mod tidy
```

## Disclosure Policy

- **Coordinated Disclosure**: We follow responsible disclosure practices
- **Public Advisories**: Security fixes are announced via GitHub Security Advisories
- **CVE Assignment**: We request CVE IDs for significant vulnerabilities
- **Release Notes**: Security fixes are clearly marked in release notes

## Security Updates

Security updates are released as:

1. **Patch Releases**: For critical vulnerabilities (e.g., 1.0.1, 1.0.2)
2. **Security Advisories**: Published on GitHub Security Advisories
3. **Notifications**: GitHub watch notifications for security updates

To receive security updates:

- Watch this repository for security notifications
- Subscribe to GitHub Security Advisories
- Check release notes regularly

## Compliance

This project follows:

- **OWASP Guidelines**: Web application security best practices
- **CIS Benchmarks**: Container security guidelines
- **Go Security Best Practices**: Official Go security recommendations

## Additional Resources

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [Elasticsearch Security](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-overview.html)
- [Docker Security Best Practices](https://docs.docker.com/engine/security/)
- [Kubernetes Security Best Practices](https://kubernetes.io/docs/concepts/security/)

## Questions?

If you have questions about security that are not covered here:

- Check our [Documentation](README.md)
- Open a [Discussion](../../discussions)
- Contact the maintainers

---

*Last updated: April 6, 2026*
