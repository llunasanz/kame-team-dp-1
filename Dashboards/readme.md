# Pasos a seguir

1. Ejecuta el siguiente comando para crear la imagen de la interfaz de cliente:
```sh
$ docker build -t html-server-image:cliente .

```
2. Ejercuta el siguiente comando para crear la imagen de la interfaz de zurich:
```sh
$ docker build -t html-server-image:zurich .
```

3. Ejecuta el contenedor que contiene el HTML:
```sh
$ docker run -d -p 80:80 html-server-image:v1
```

4. Entra en localhost:80/cliente.html para la interfaz de cliente.
5. Entra en localhost:80/zurich.html para la interfaz de zurich.



