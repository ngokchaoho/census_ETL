# census_ETL
Create ETL pipeline using airflow in Docker

This is tested in 
```
WSL version: 2.0.9.0
Kernel version: 5.15.133.1-1
WSLg version: 1.0.59
MSRDC version: 1.2.4677
Direct3D version: 1.611.1-81528511
DXCore version: 10.0.25131.1002-220531-1700.rs-onecore-base2-hyp
Windows version: 10.0.22621.2715
```
To see the result:
```
docker compose up airflow-init
docker compose up
```
And you should be able to see the result in localhost browser on `127.0.0.1:8080` with user and password `airflow`
