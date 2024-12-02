# OpenEO ArgoWorkflows Api

This is an implementation of the OpenEO Api. This subdirectory implements ( in python ) the **api** server, a **redis worker**, and a psql **migration**. These are all currently housed here, as they use the same dependencies, and due to the minimal code of the queue and the migration, the docker environment for the api is reused.

**Api**: Serves api requests, is able to retrieve files from the FS mount. Can write jobs to PostgreSQL, but does not directly submit them to Argo. It can get logs from Argo.

**Redis worker**: Manages job state. Created jobs are queued, and when there is room in Argo, submitted. The job will be marked according to it's completion from Argo.

**Migration**: Run before each deployment to migrate the database to be consistent with the latest changes. It is managed via alembic.


## Development

Open this directory in vscode.

```
code -n .
```

From vscode.
```
ctrl + l-shift + p
```

If the image has been built before, select:
```
>Dev containers: Reopen in container
```
else, select:
```
>Dev containers: Rebuild and Reopen in container
```

Create the venv using poetry.
```
poetry install --all-extras
```

You can create a .notebooks directory which will be git ignored. Here you can create jupyternotebooks if you want to write and develop code interactively. There is a docker-compose to develop at an integration level, but this will be standardised with some integration tests, and then will be documented.

###

Tests are run via pytest and should be discoverable via vscode. 

Otherwise:
```
pytest ./tests
```