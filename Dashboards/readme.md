# Pasos a seguir

1. Ejecuta el siguiente comando para crear la imagen de las interfaces:
```sh
$ docker build -t html-server-image:dashboards .
```

2. Ejecuta el contenedor que contiene el HTML:
```sh
$ docker run -d -p 80:80 html-server-image:dashboards
```

3. Entra en localhost:80/cliente.html para la interfaz de cliente.
4. Entra en localhost:80/zurich.html para la interfaz de zurich.



