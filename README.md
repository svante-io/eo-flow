<picture>
  <source media="(prefers-color-scheme: dark)" width="800px" srcset="https://github.com/svante-io/eo-flow/raw/main/logo-dark.png">
  <img alt="eoflow logo" width="800px" src="https://github.com/svante-io/eo-flow/raw/main/logo-light.png">
</picture>

# EO-Flow: end-to-end Earth Observation DataOps

[![License][license badge]][license]
![Coverage][coverage badge]
![Status][status badge]

[license badge]: https://img.shields.io/badge/License-Apache%202.0-blue.svg
[license]: https://opensource.org/licenses/MIT


[coverage badge]: https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/Lkruitwagen/26eb1f55bf4f0791d6a0a70ee0dec834/raw/eoflow-coverage-blob.json

[status badge]: https://img.shields.io/badge/under%20construction-ffae00


EO-Flow is a platform for end-to-end EO+ML (Earth Observation + Machine Learning).
EO-Flow aims to be the easiest and most cost-effective way to develop insights from satellite data, unlocking scale: large area, deep temporal, or high-cadence deployments.
With a low-code client library, and an accessible UI, EO-Flow allows your team to focus on the insight, not on the infrastructure.

- **Materialize** an ML-ready dataset into a cloud environment of your choice.
- **Train** a machine learning model on your dataset using an eo-flow dataloader
- **Deploy** your ML model on a (different) target area, trusting that your data
- **Visualise & Share** your ML inference with your team and stakeholders.

Materialize, train, and deploy jobs can be dispatched with either *eager* or *patient* priority. Eager jobs will execute immediately; patient jobs will make use of cost-optimal pre-emptible compute resources.

## Roadmap & Coverage

- Testing on CI
- client library and data service
- PyPi listing
- Eager training on GCP
- Eager deployment on GCP
- support for COG
- support for custom COG / custom catalog

**Current coverage:**

|    Job   |      GCP     |      AWS       |        Azure     |
| -------- | ------------ | -------------- | ---------------- |
| Register | Eager :o: Patient :o: | Eager :o: Patient :o: |  Eager :o: Patient :o: |
| Materialize | Eager :white_check_mark: Patient :o:    | Eager :o: Patient :o: |  Eager :o: Patient :o: |
| Train    | Eager :o: Patient :o: |  Eager :o: Patient :o: |  Eager :o: Patient :o: |
| Deploy    | Eager :o: Patient :o: |  Eager :o: Patient :o: |  Eager :o: Patient :o: |


## Getting Started

We'll be distributing via PyPi soon! For now just:

    pip install -e ".[core]"

It's just [Dagster](https://dagster.io/)! See the available jobs using the dagster terminal:

    dagster dev -m eoflow.definitions

### EO-Flow Client

:construction:


### Self-Hosting

## Development

&copy; 2024 svante.io
