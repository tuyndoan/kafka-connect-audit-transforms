# Contributing to Kafka Connect Audit Transforms

Thank you for your interest in contributing to this project! We welcome contributions from the community.

## How to Contribute

### Reporting Bugs

If you find a bug, please create an issue with:
- A clear, descriptive title
- Steps to reproduce the issue
- Expected behavior vs actual behavior
- Environment details (Java version, Kafka Connect version, etc.)
- Relevant logs or error messages

### Suggesting Enhancements

We welcome suggestions for new features or improvements. Please create an issue with:
- A clear description of the enhancement
- Use cases and examples
- Any potential implementation ideas (optional)

### Code Contributions

#### Prerequisites

- Java 8 or higher
- Maven 3.6+
- Git

#### Development Setup

1. **Fork the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/kafka-connect-audit-transforms.git
   cd kafka-connect-audit-transforms
   ```

2. **Build the project**
   ```bash
   mvn clean install
   ```

3. **Run tests**
   ```bash
   mvn test
   ```

#### Making Changes

1. **Create a branch**
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

2. **Make your changes**
   - Follow existing code style and conventions
   - Write or update tests for new functionality
   - Update documentation if needed
   - Ensure all tests pass: `mvn test`

3. **Commit your changes**
   - Write clear, descriptive commit messages
   - Follow conventional commit format when possible:
     - `feat: add new feature`
     - `fix: fix bug description`
     - `docs: update documentation`
     - `test: add test cases`
     - `refactor: refactor code`

4. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

5. **Create a Pull Request**
   - Provide a clear description of your changes
   - Reference any related issues
   - Ensure CI checks pass

#### Code Style Guidelines

- Follow Java naming conventions
- Use meaningful variable and method names
- Add JavaDoc comments for public classes and methods
- Keep methods focused and concise
- Handle errors appropriately with meaningful error messages

#### Testing

- Write unit tests for new functionality
- Ensure all existing tests pass
- Test edge cases and error conditions
- Update or add integration test examples if applicable

### Pull Request Process

1. Ensure your code follows the project's style guidelines
2. Update documentation if you've changed functionality
3. Add tests for new features
4. Ensure all tests pass and there are no linting errors
5. Update the CHANGELOG.md if applicable (if the project has one)
6. Request review from maintainers

### Review Process

- Maintainers will review your PR
- Address any feedback or requested changes
- Once approved, your PR will be merged

## Questions?

If you have questions about contributing, feel free to:
- Open an issue with the `question` label
- Contact the maintainer at doanvantuyn@gmail.com

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

Thank you for contributing! 🎉

