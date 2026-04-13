# Setup

After a fresh docker compose up, you need to initialize the auth database and create your first admin.

Run this once:

    docker compose exec dagster_webserver dagster-authkit init-db --with-admin

This will prompt you to set an admin username and password.

# Managing accounts and roles

All user management is done via CLI commands on the webserver container:

```
# Create a user with a specific role
docker compose exec dagster_webserver dagster-authkit add-user alice --role editor

# List all users
docker compose exec dagster_webserver dagster-authkit list-users

# Reset a user's password
docker compose exec dagster_webserver dagster-authkit change-password alice

# View RBAC permissions
docker compose exec dagster_webserver dagster-authkit list-permissions
```

# RBAC roles

  dagster-authkit provides 4 roles with increasing privileges:

  ┌──────────┬─────────────────────────────────────────────────────────────────┐
  │   Role   │                           Permissions                           │
  ├──────────┼─────────────────────────────────────────────────────────────────┤
  │ Viewer   │ Read-only access to the UI (view runs, assets, logs)            │
  ├──────────┼─────────────────────────────────────────────────────────────────┤
  │ Launcher │ Viewer + can launch/terminate runs and trigger materializations │
  ├──────────┼─────────────────────────────────────────────────────────────────┤
  │ Editor   │ Launcher + can modify schedules, sensors, and configuration     │
  ├──────────┼─────────────────────────────────────────────────────────────────┤
  │ Admin    │ Full access including user management                           │
  └──────────┴─────────────────────────────────────────────────────────────────┘

  Permissions are enforced by intercepting GraphQL mutations — unauthorized actions are blocked before
  they reach Dagster.
