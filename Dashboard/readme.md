# Pasos a seguir

1. Ejecuta el siguiente comando:
```sh
$ docker build -t html-server-image:v1 .
```
2. Ejecuta el contenedor que contiene el HTML:
```sh
$ docker run -d -p 80:80 html-server-image:v1
```
3. Entra en localhost:80



