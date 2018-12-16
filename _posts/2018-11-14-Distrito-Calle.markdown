---
layout:     post
title:      Calles más accidentadas (según el distrito)
tags: 	Spark AWS Calle Distrito
subtitle:  Programa que muestra las calles en las que se producen mayor cantidad de accidentes, según el distrito especificado
category:  project1
---
<!-- Start Writing Below in Markdown -->

# Descripción del Programa
Este programa permite obtener las calles más accidentadas en un distrito descrito. Además de dar el nombre de la calle donde se producen, se proporcionan el número de accidentes que se han producido en ella.
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

spark-submit distrito_calle.py (file) (distrito)

{% endhighlight %}

- El contenido ***file*** representa el archivo que queremos procesar y es importante que el formato de este archivo sea **csv**.
- La variable ***distrito*** es necesario especificarla. Para saber los que estan disponibles, recomendamos ir a la sección de **Descripción del programa** en esta misma página.

Si estamos probando el programa dentro de un **cluster**, podemos definir el numero de ***workers*** y de ***worker-nodes***. Para ello tendremos que añadir estos parametros a la linea de comando anterior:

{% highlight shell %}

--num-executors (number) --executor-cores (number)

{% endhighlight %}


# Salida del programa

{% highlight python %}


LUGAR ACCIDENTE                                      COUNT  
CALLE  GRAN VIA NUM                            ...   1074
PASEO DEL PRADO NUM                            ...    646
CALLE DE ATOCHA NUM                            ...    605
PASEO DE RECOLETOS NUM                         ...    395
CALLE DE SAN BERNARDO NUM                      ...    366
CALLE DE TOLEDO NUM                            ...    341
PLAZA DEL EMPERADOR CARLOS V NUM               ...    339
CALLE DE ALCALA NUM                            ...    329
PLAZA DE CIBELES NUM                           ...    252
CALLE  MAYOR NUM                               ...    246
CALLE DE LA PRINCESA NUM                       ...    215
CALLE  GRAN VIA - CALLE DE SAN BERNARDO        ...    195
PLAZA DE ESPAÑA NUM                            ...    191
CALLE DE BAILEN NUM                            ...    175
CALLE DE ALCALA - PLAZA DE CIBELES             ...    169
CALLE DE SEGOVIA NUM                           ...    166
PLAZA DE CANOVAS DEL CASTILLO NUM              ...    160
PASEO DEL PRADO - PLAZA DE CANOVAS DEL CASTILLO...    158
CALLE DE PRIM - PASEO DE RECOLETOS             ...    156
CUESTA DE SAN VICENTE NUM                      ...    154
RONDA DE ATOCHA NUM                            ...    153
CALLE DE SEGOVIA - RONDA DE SEGOVIA            ...    137
CALLE DE FUENCARRAL NUM                        ...    126
GRAN VIA DE SAN FRANCISCO NUM                  ...    119
CALLE DE GENOVA NUM                            ...    116
CALLE DE ALBERTO AGUILERA NUM                  ...    113
CALLE DE SAGASTA NUM                           ...    113
PLAZA DE COLON NUM                             ...    108
CALLE DE BARBARA DE BRAGANZA - PASEO DE RECOLET...    104
PASEO DE RECOLETOS - PLAZA DE CIBELES          ...    104
...                                                   ...
CALLE DEL MARQUES DE CUBAS - CALLE DE ZORRILLA ...      1
CALLE DE ATOCHA - SUBTERRANEO                  ...      1
CALLE DE MANUELA MALASAÑA - CALLE DE SAN BERNAR...      1
CALLE DE LA MONTERA - CALLE DE LOS JARDINES    ...      1
CALLE DE CARRETAS - CALLE DEL SOL              ...      1
PLAZA DE LA VILLA NUM                          ...      1
CALLE DE LA FARMACIA NUM                       ...      1
CALLE DE ALGECIRAS - RONDA DE SEGOVIA          ...      1
CUESTA DE LA VEGA - PARQUE DEL EMIR MOHAMED I  ...      1
PLAZA DE SAN ILDEFONSO - CALLE DE DON FELIPE   ...      1
PLAZA DE SANTA MARIA SOLEDAD TORRES ACOSTA NUM ...      1
ESTACION DE METRO PRINCIPE PIO - PASEO DE LA VI...      1
CALLE DE CALATRAVA - CALLE DEL ANGEL           ...      1
CALLE DE ATOCHA - CALLE DEL PRADO              ...      1
CALLE DE SAN LORENZO - CALLE DE SAN MARCOS     ...      1
CUESTA DE SAN VICENTE - PASEO DEL EMBARCADERO  ...      1
CALLE DE SAN ANDRES - CALLE DEL ESPIRITU SANTO ...      1
CALLE DEL ESCORIAL NUM                         ...      1
CALLE DE SERRANO JOVER - CALLE DE LA PRINCESA  ...      1
CALLE DEL MOLINO DE VIENTO - CALLE DE DON FELIP...      1
CALLE DE BAILEN - CALLE DE ARRIETA             ...      1
CALLE DEL FACTOR - CALLE  MAYOR                ...      1
PLAZA DE ISABEL II - CALLE DE ARRIETA          ...      1
CALLE DE BAILEN - CALLE DE REQUENA             ...      1
CALLE DE LA MAGDALENA - CALLE DE LAVAPIES      ...      1
CALLE DE LA BALLESTA - CALLE DEL DESENGAÑO     ...      1
CUESTA DE SAN VICENTE - AUTOVIA  M-30 CALZADA 1...      1
CALLE DE COLON - CALLE DE FUENCARRAL           ...      1
CALLE DE BAILEN - CALLE DE CALATRAVA           ...      1
GRAN VIA DE SAN FRANCISCO - CARRERA DE SAN FRAN...      1

{% endhighlight %}

### Chamartin

<img src="https://artuyero.github.io/Cloud_BigData_UCM/img/graphics/distrito_calle(CHAMARTIN).png" height="150%" width="150%">

### Moncloa-Aravaca

<img src="https://artuyero.github.io/Cloud_BigData_UCM/img/graphics/distrito_calle(MONCLOA).png" height="150%" width="150%">



[1]:https://artuyero.github.io/Cloud_BigData_UCM//about/
[2]: