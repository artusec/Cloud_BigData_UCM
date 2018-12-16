---
layout:     post
title:      Número de accidentes en una calle y rango horario en el que se produce en un distrito y dia determinados
tags: 		Spark AWS Hora Dia Distritos Calles
subtitle:  	Muestra el numero de accidentes en un determinado rango horario de una calle a partir de un distrito y dia especificados
category:  project1
---
<!-- Start Writing Below in Markdown -->

# Descripción del Programa
Mediante este programa podemos saber el número de accidentes que se producen en una determinada calle y su rango horario asociado, segun el distrito y dia especificados. Por tanto, las calles mostradas pertenecerán al distrito determinado.
Por lo que la salida estara formada por la calle, el rango horario asociado a esa calle y el número de accidentes producidos.
Para saber los distritos disponibles en los datos o la forma en la que se escriben, recomendamos la ejecución del programa inline `show_districts.py` o su descripción en [esta web][2]
# Como ejecutarlo

## 1. Modo Local
Para ejecutar este programa necesitamos tener instalado en nuestro ordenador **Spark en Modo Local**. Además, necesitariamos tener instalado el paquete de ***Pandas*** de Python, que se descarga de la siguiente manera:

{% highlight shell %}

sudo apt-get install python-pip
sudo pip install numpy
sudo pip install pandas

{% endhighlight %}

## 2. Modo Cluster
Para ejecutarlo en este modo necesitariamos un ***cluster*** en AWS (Amazon Web Services), utilizando el modulo EMR (***Elastic Map Reduce***). En cuanto a las especificaciones del ***cluster***, se encuentran detalladas en la pagina [descripción][1] de esta web.



Una vez que ya hemos elegido el modo de ejecución y revisado todos los elementos, nos disponemos a ejecutar el programa. Para ello, utilizaremos la siguiente linea de comando: 

{% highlight shell %}

spark-submit horasLugares.py (file) (distrito) (dia de la semana)

{% endhighlight %}

- El contenido ***file*** representa el archivo que queremos procesar y es importante que el formato de este archivo sea **csv**.
- En el campo de ***dia de la semana***, hay que incluir el dia de la semana que queremos analizar (lunes, martes,etc.). **IMPORTANTE:** para que funcione el dia tiene que estar escrito en español
- La variable ***distrito*** es necesario especificarla. Para saber los que estan disponibles, recomendamos ir a la sección de **Descripción del programa** en esta misma página.


Si estamos probando el programa dentro de un **cluster**, podemos definir el numero de ***workers*** y de ***worker-nodes***. Para ello tendremos que añadir estos parametros a la linea de comando anterior:

{% highlight shell %}

--num-executors (number) --executor-cores (number)

{% endhighlight %}


# Salida del programa

{% highlight python %}

CENTRO | MARTES

LUGAR ACCIDENTE                                    RANGO HORARIO       COUNT 
AVENIDA DE PORTUGAL FL KM.                     ... DE 00:00 A 00:59      3
AVENIDA DE PORTUGAL FT KM.                     ... DE 9:00 A 9:59        7
                                                   DE 8:00 A 8:59        4
                                                   DE 7:00 A 7:59        3
                                                   DE 17:00 A 17:59      1
CALLE  GRAN VIA - CALLE DE ALCALA              ... DE 11:00 A 11:59      5
                                                   DE 20:00 A 20:59      4
                                                   DE 15:00 A 15:59      2
                                                   DE 6:00 A 6:59        2
CALLE  GRAN VIA - CALLE DE CONCEPCION ARENAL   ... DE 14:00 A 14:59      2
CALLE  GRAN VIA - CALLE DE GONZALO JIMENEZ DE Q... DE 19:00 A 19:59      2
                                                   DE 23:00 A 23:59      2
CALLE  GRAN VIA - CALLE DE HORTALEZA           ... DE 22:00 A 22:59      4
                                                   DE 21:00 A 21:59      2
CALLE  GRAN VIA - CALLE DE LOS TUDESCOS        ... DE 14:00 A 14:59      2
CALLE  GRAN VIA - CALLE DE MESONERO ROMANOS    ... DE 7:00 A 7:59        2
CALLE  GRAN VIA - CALLE DE SAN BERNARDO        ... DE 3:00 A 3:59        8
                                                   DE 18:00 A 18:59      7
                                                   DE 17:00 A 17:59      6
                                                   DE 12:00 A 12:59      4
                                                   DE 19:00 A 19:59      4
                                                   DE 21:00 A 21:59      3
                                                   DE 11:00 A 11:59      2
                                                   DE 20:00 A 20:59      2
                                                   DE 22:00 A 22:59      2
                                                   DE 8:00 A 8:59        2
                                                   DE 9:00 A 9:59        2
CALLE  GRAN VIA - CALLE DE SAN LEONARDO        ... DE 19:00 A 19:59      3
CALLE  GRAN VIA - CALLE DE SILVA               ... DE 18:00 A 18:59      2
CALLE  GRAN VIA - CALLE DE VALVERDE            ... DE 19:00 A 19:59      3


{% endhighlight %}

[1]:https://artuyero.github.io/Cloud_BigData_UCM//about/
[2]: