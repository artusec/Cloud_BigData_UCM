---
layout: page
title: "Descripción"
description: "Explicación sobre diferentes aspectos del proyecto"
header-img: "img/headers/header-madrid_2_prueba.png"
---

Nuestro proyecto consiste en analizar los datos obtenidos de la [sede de datos de Madrid][1] sobre la accidentalidad en la ciudad de Madrid desde 2010 hasta la actualidad (ultima actualización de los ficheros). En estos datos tenemos información sobre el lugar donde se produjo, cuando (fecha y rango de hora), condiciones metorologicas,etc. Toda la descripción de los datos se encuentra [aqui][2]. También, recordamos que estos datos pueden ser utilizados para fines academicos, como es el caso, o, también, para uso comercial y no comercial. Toda esta informacion queda recogida en [esta pagina][4]

Todos los programas realizados se encuentran en la carpeta /Scripts del repositorio, pero en la seccion [Proyecto][3] de esta página, hemos explicado el funcionamiento de cada uno de ellos. Además, detallamos la salida que ofrece y, en algunos casos, representamos la salida en formato gráfico.

Tambien, hemos realizado un ***script*** llamado `accidentalidad_app` donde centralizamos todos los programas creados. Por lo que podemos ejecutar todos los programas, mediante ese ***script***.

## Herramientas
Para poder realizar este análisis y tratamiento de los datos, hemos utilizado Spark con Python (***v.3.7.1***) (usando la libreria de Pandas) en un cluster de Spark en AWS (Amazon Web Service).
Las especificaciones de la maquina utilizada son las siguientes:

Sección | Configuración
|---------|:----------|
Release   | emr-5.8.0
Applications    | Spark
Tipo de Instancias  | m4.xlarge
Numero de Instancias  | 3 o 5, según los tiempos

**NOTA:** Nuestra intención era realizar un análisis de tiempos de ejecución por cada programa, cambiando diferentes configuraciones (numero de instancias, ***executors*** y ***executors-core***). Pero, al no encontrar ninguna manera de obtener los tiempos ejecución, no hemos podido realizarlo.

Para poder generar las gráficas mostradas hemos utilizado el modulo ***plot*** de Python. Para instalarlo, debes ejecutar el siguiente comando:

{% highlight shell %}

sudo pip install plot

{% endhighlight %}


El contenido, tanto de esta Web, como del repositorio, es parte de la realización de un proyecto para la asignatura de Cloud & Big Data de la Universidad Complutense de Madrid.
	









[1]:https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=7c2843010d9c3610VgnVCM2000001f4a900aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD&vgnextfmt=default
[2]:https://datos.madrid.es/FWProjects/egob/Catalogo/Seguridad/Ficheros/Documento_estructura_accidentes_trafico_v1.pdf
[3]:https://artuyero.github.io/Cloud_BigData_UCM//projects/project1/
[4]:https://datos.madrid.es/portal/site/egob/menuitem.400a817358ce98c34e937436a8a409a0/?vgnextoid=b4c412b9ace9f310VgnVCM100000171f5a0aRCRD&vgnextchannel=b4c412b9ace9f310VgnVCM100000171f5a0aRCRD&vgnextfmt=default