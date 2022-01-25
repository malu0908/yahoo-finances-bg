# yahoo-finances-bg

![GitHub repo size](https://img.shields.io/github/repo-size/iuricode/README-template?style=for-the-badge)
![GitHub forks](https://img.shields.io/github/forks/iuricode/README-template?style=for-the-badge)
![Bitbucket open issues](https://img.shields.io/bitbucket/issues/iuricode/README-template?style=for-the-badge)
![Bitbucket open pull requests](https://img.shields.io/bitbucket/pr-raw/iuricode/README-template?style=for-the-badge)

![arquitetura-yfbg](https://user-images.githubusercontent.com/56079012/151059283-c7d301e9-4957-48da-a92d-58af27fa9e53.png)

## Description
Use case for an ELT data pipeline, where data is pulled from the asset information platform  Yahoo Finances, processed with Spark on the free Databricks Community cluster, and saved in delta format, following Delta Lake architecture.

## Important commands

Install airflow on kubernetes with values.yaml
```
helm install flower -n airflow bitnami/airflow --values values.yaml
```

Create configMap to requirements.txt

```
kubectl create -n airflow configmap requirements --from-file=requirements.txt
```
