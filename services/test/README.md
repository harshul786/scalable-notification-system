# Test Service Documentation

#

# This service runs the integration test suite against the running services.

# It's optional and can be enabled via Docker Compose profiles.

#

# The test service:

# - Depends on all core services being healthy

# - Runs from the integration-tests.js file

# - Reports results back to console

# - Exits with proper status codes

#

# See docker-compose.yml for RUN_TESTS variable usage

See ../.. for test files and configuration
