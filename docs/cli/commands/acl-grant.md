# `mycel acl grant`

Grants a system role or space permission.

## Examples

```sh
mycel -d ./data -u admin -p change-me acl grant system --user-id <user_id> --role user_admin
mycel -d ./data -u admin -p change-me acl grant space --space-id <space_id> --user-id <user_id> --permission read
```

## Values

System roles: `superuser`, `user_admin`, `operator`.

Space permissions: `read`, `write`, `admin`.
