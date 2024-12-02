# OpenEO ArgoWorkflows

OpenEO Argoworkflows is an implementation of the [OpenEO Api]() and [OpenEO Processes]() specifications. This repository implements two components, an api server, and an executor. The Api implementation is based on the [OpenEO Fastapi]() package, and the Executor implementation is based on [OpenEO Processes Dask]() and [OpenEO PG Parser]().

The two components here, are integrated, and expected to be installed via the [OpenEO ArgoWorkflows Helm Chart](). The helm chart a number of dependencies are installed and configured to work with the components implemented here.


## Development

In the respective documentary for the api and executor there is a dockerfile defined that can be used as a development environment for each component.

To work on the Api
```
cd ./openeo_argoworkflows/api
```

To work on the Executor
```
cd ./openeo_argoworkflows/api
```

From each of these directories, there is a .devcontainer configuration. Openining these sub directories in vscode will display the option to open the development container. It is intentional that each of these components have seperate development environments. Source and test code is available in each respective sub repo. **Note**: there are currently no tests for the executor.

## Release

Each component is released using the version number found in its respective pyproject.toml, and prefixed with the name of the component. The version numbers in the toml follow [Calendar Versioning](https://calver.org/) where the Major version is the year, the Minor version the month, and the Mirco version is the index of the release that month, if it is the first release that month, the Micro is 1.

Format:
`( api | executor )-YYYY.MM.MICRO`s