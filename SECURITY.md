# Security Policy

## Supported Versions

We actively support and provide security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 2.0.x   | :white_check_mark: |
| < 2.0   | :x:                |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security vulnerability, please follow these steps:

### How to Report

1. **Do NOT** create a public GitHub issue for security vulnerabilities
2. Email the security team directly at: **doanvantuyn@gmail.com**
3. Include the following information:
   - Description of the vulnerability
   - Steps to reproduce the issue
   - Potential impact
   - Suggested fix (if any)
   - Your contact information (optional, for follow-up questions)

### What to Expect

- You will receive an acknowledgment within 48 hours
- We will investigate and assess the vulnerability
- We will keep you informed of the progress
- Once fixed, we will:
  - Credit you in the security advisory (if you wish)
  - Release a patch version with the fix
  - Publish a security advisory

### Disclosure Policy

- We follow **responsible disclosure** practices
- Please allow us reasonable time to address the vulnerability before public disclosure
- We aim to address critical vulnerabilities within 7-14 days
- We will coordinate with you on the disclosure timeline

### Security Best Practices

When using this project:

1. **Keep dependencies updated**: Regularly update to the latest version
2. **Review configurations**: Ensure your Kafka Connect configuration follows security best practices
3. **Monitor logs**: Watch for unusual behavior or errors
4. **Use in trusted environments**: This SMT processes data from your databases - ensure proper access controls
5. **Validate inputs**: While the SMT handles Debezium data, ensure your source connectors are properly configured

### Known Security Considerations

- This SMT processes data from Debezium CDC streams
- Ensure proper authentication and authorization at the Kafka Connect level
- Sensitive data in audit records should be handled according to your organization's data protection policies
- Consider encryption in transit and at rest for audit data

## Security Updates

Security updates will be:
- Released as patch versions (e.g., 2.0.1 → 2.0.2)
- Documented in release notes
- Tagged with security labels in GitHub releases

Thank you for helping keep this project secure!

