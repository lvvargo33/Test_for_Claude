# SECURITY NOTICE: Service Account Key Exposed

## What Happened
The service account key `location-optimizer-1-449414f93a5a.json` was disabled because it was exposed publicly. Google automatically detects and disables exposed credentials to protect your account.

## Immediate Actions Required

1. **Generate New Credentials** (see generate_new_credentials_guide.md)

2. **Never Commit Credentials to Git**
   - Add to `.gitignore`:
   ```
   *.json
   *credentials*
   *service-account*
   ```

3. **Use Environment Variables Instead**
   - Store credentials outside the repository
   - Reference via `GOOGLE_APPLICATION_CREDENTIALS` environment variable

4. **Consider Using Workload Identity** (for production)
   - More secure than service account keys
   - No keys to manage or expose

## Best Practices Going Forward

1. **Always encrypt sensitive files**:
   ```bash
   gpg --symmetric --cipher-algo AES256 your-credentials.json
   ```

2. **Use secret management services**:
   - Google Secret Manager
   - GitHub Secrets (for CI/CD)
   - Environment variables

3. **Audit your repository**:
   - Check git history for other exposed secrets
   - Use tools like `git-secrets` or `truffleHog`

4. **Set up pre-commit hooks** to prevent accidental commits of secrets

## Clean Up Git History (if needed)
If the credentials were committed to git history:
```bash
# Remove from all history (destructive - backup first!)
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch location-optimizer-1-449414f93a5a.json" \
  --prune-empty --tag-name-filter cat -- --all
```