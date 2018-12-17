---
layout:     post
title:      Calles más Accidentadas (según el dia y el distrito)
tags: 		Spark AWS Dia Distritos 
subtitle:  	Muestra las calles con mayor cantidad de accidentes, según el día y distritos especificados
category:  project1
---
<!-- Start Writing Below in Markdown -->

# Descripción del Programa
Este programa permite obtener las calles mas accidentadas de la ciudad de Madrid, según el dia y distritos que especifiquemos. Además de mostrar el nombre de la calle, muestra el número de accidentes producidos en esa calle.
Para saber los distritos disponibles en los datos o la forma en la que se escriben, recomendamos la ejecución del programa `show_districts.py` o su descripción en [esta web][2]
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

spark-submit chooseStreet.py (file) (distrito) (dia de la semana)

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

                                      LUGAR ACCIDENTE    ...      PERCENT
5                          AUTOVIA  M-30 CALZADA 2 KM.    ...     5.215054
4                          AUTOVIA  M-30 CALZADA 1 KM.    ...     4.032258
172                           CALLE DE LA PRINCESA NUM    ...     3.763441
259                          CARRETERA DE CASTILLA KM.    ...     3.333333
267                          CUESTA DE SAN VICENTE NUM    ...     2.526882
65                      AVENIDA DEL PADRE HUIDOBRO NUM    ...     2.365591
29                          AVENIDA DE LA VICTORIA NUM    ...     2.043011
12                            AVENIDA  COMPLUTENSE NUM    ...     1.505376
64                      AVENIDA DEL PADRE HUIDOBRO KM.    ...     1.397849
34                  AVENIDA DE LOS REYES CATOLICOS NUM    ...     1.344086
306                       PASEO DEL PINTOR ROSALES NUM    ...     1.290323
126     CALLE DE FERRAZ - CALLE DEL MARQUES DE URQUIJO    ...     1.129032
135                     CALLE DE FRANCOS RODRIGUEZ NUM    ...     1.129032
92                        CALLE DE ANTONIO MACHADO NUM    ...     1.129032
290                            PASEO DE LA FLORIDA NUM    ...     1.075269
295                             PASEO DE RUPERTO CHAPI    ...     0.913978
58                 AVENIDA DEL ARCO DE LA VICTORIA NUM    ...     0.860215
130                                CALLE DE FERRAZ NUM    ...     0.860215
310                                PLAZA DE ESPAÑA NUM    ...     0.860215
42                     AVENIDA DE PUERTA DE HIERRO KM.    ...     0.806452
56                           AVENIDA DE VALLADOLID NUM    ...     0.806452
71                                 CALLE  30  18RN KM.    ...     0.752688
124          CALLE DE FERRAZ - CALLE DE ROMERO ROBLEDO    ...     0.752688
17                      AVENIDA DE JUAN DE HERRERA NUM    ...     0.752688
214                       CALLE DE SINESIO DELGADO NUM    ...     0.698925
35    AVENIDA DE MIRAFLORES - CALLE DE SINESIO DELGADO    ...     0.645161
241  CALLE DEL MARQUES DE URQUIJO - PASEO DEL PINTO...    ...     0.645161
46          AVENIDA DE SENECA - CALLE DE MARTIN FIERRO    ...     0.645161
22     AVENIDA DE LA OSA MAYOR - CALLE DEL PICO OCEJON    ...     0.591398
312                        PLAZA DEL CARDENAL CISNEROS    ...     0.591398

{% endhighlight %}


### Moncloa-Aravaca | Lunes

<img src="https://artuyero.github.io/Cloud_BigData_UCM/img/graphics/chooseStreet(Moncloa)(Lunes).png" height="150%" width="150%">

### Centro | Miércoles

<img src="https://artuyero.github.io/Cloud_BigData_UCM/img/graphics/chooseStreet(CENTRO)(Miercoles).png" height="150%" width="150%">


[1]:https://artuyero.github.io/Cloud_BigData_UCM//about/
[2]:https://artuyero.github.io/Cloud_BigData_UCM//project1/2018/11/22/Show-Districts/


