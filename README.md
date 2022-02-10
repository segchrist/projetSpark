# projetSpark : Analyse des facteurs aggravant des accidents aux état-unis
## Présentation du dataset

Il s'agit d'un ensemble de données sur les accidents de voiture à l'échelle nationale, qui couvre 49 États des États-Unis. Les données sur les accidents sont collectées de février 2016 à déc 2020, à l'aide de plusieurs API qui fournissent des données sur les incidents (ou événements) de circulation en continu. Ces API diffusent des données de trafic capturées par diverses entités, telles que les départements des transports des États-Unis et des États, les forces de l'ordre, les caméras de trafic et les capteurs de trafic au sein des réseaux routiers. Actuellement, cet ensemble de données contient environ 1,5 million d'enregistrements d'accidents.

ID : Il s'agit d'un identifiant unique de l'enregistrement de l'accident.
Severity : Indique la gravité de l'accident, un nombre entre 1 et 4.
Start_Time : Indique l'heure de début de l'accident dans le fuseau horaire local.
End_Time: Indique l'heure de fin de l'accident dans le fuseau horaire local. L'heure de fin correspond au moment où l'impact de l'accident sur le trafic a été éliminé.
Start_Lat : Indique la latitude en coordonnées GPS du point de départ.
Start_Lng :Indique la longitude en coordonnées GPS du point de départ.
End_Lat : Indique la latitude en coordonnées GPS du point final.
End_Lng : Indique la longitude en coordonnées GPS du point final.
Distance(mi) : La longueur de l'étendue de la route concernée par l'accident.
Description : Affiche la description en langage naturel de l'accident.
Number : Indique le numéro de la rue dans l'enregistrement de l'adresse.
Street : Indique le nom de la rue dans l'enregistrement de l'adresse.
Side : Indique le côté relatif de la rue (droite/gauche) dans l'enregistrement de l'adresse.
City : Indique la ville dans l'enregistrement de l'adresse.
County : Indique le comté dans l'enregistrement de l'adresse.
State : Indique l'état de la fiche d'adresse.
ZipCode : Indique le code postal dans l'enregistrement de l'adresse.
Country : Indique le pays dans l'enregistrement de l'adresse.
TimeZone : Indique le fuseau horaire en fonction de la localisation de l'accident (est, centre, etc.).
Airport Code : Indique une station météorologique basée à l'aéroport qui est la plus proche du lieu de l'accident.
Weather_Timestamp : Indique l'horodatage de l'enregistrement des observations météorologiques (en heure locale).
Température(F) : Indique la température (en Fahrenheit).
Wind_Chill(F) : Indique le refroidissement éolien (en Fahrenheit).
Humidity(%) : Indique l'humidité (en pourcentage).
Pressure(in) : Indique la pression de l'air (en pouces).
Visibility(mi) : Indique la visibilité (en miles).
Wind_Direction : Indique la direction du vent.
Wind_Speed(mph) : Indique la vitesse du vent (en miles par heure).
Precipitation(in) : Indique la quantité de précipitations en pouces, s'il y en a.
Weather_Condition : Indique les conditions météorologiques (pluie, neige, orage, brouillard, etc.).
Amenity : Une annotation de POI qui indique la présence d'une commodité dans un endroit proche.
Bump : Une annotation de POI qui indique la présence d'un dos d'âne ou d'une bosse dans un endroit proche.
Crossing : Une annotation de POI qui indique la présence d'un croisement dans un endroit proche.
Give_Way : Une annotation de POI qui indique la présence de give_way dans un endroit proche.
Junction : Une annotation de POI qui indique la présence d'une jonction dans un endroit proche.
No_Exit : Une annotation de POI qui indique la présence d'une jonction dans un endroit proche. 
Railway : Une annotation de POI qui indique la présence d'un chemin de fer dans un endroit proche.
Roundabout : Une annotation de POI qui indique la présence d'un rond-point dans un endroit proche
Station : Une annotation de POI qui indique la présence d'une station dans un endroit proche.
Stop : Une annotation de POI qui indique la présence d'un arrêt dans un endroit proche.
Traffic_Calming: Une annotation de POI qui indique la présence d'un système de modération du trafic dans un lieu proche.
Traffic_Signal : Une annotation de POI qui indique la présence d'un signal de circulation dans un endroit proche.
Turning_Loop : Une annotation POI qui indique la présence de turning_loop dans un endroit proche.
Sunrise_Sunset : Indique la période de la journée (c'est-à-dire le jour ou la nuit) en fonction du lever/coucher du soleil.
Civil_Twilight : Indique la période du jour (c'est-à-dire le jour ou la nuit) en fonction du crépuscule civil.
Nautical_Twilight : Indique la période du jour (c'est-à-dire le jour ou la nuit) en fonction du crépuscule nautique.
Astronomical_Twilight : Indique la période de la journée (c'est-à-dire le jour ou la nuit) en fonction du crépuscule astronomique.


## Présentation des axes d'analyses et problématiques

### Axe aménagement des routes
On a essayé de trouver quels étaient facteurs qui causaient et aggravaient les accidents de la route aux états unis

### Axe météorologique
On a analysé les conditions météo qui favorisaient et aggravaient les accidents

### Axe géographique
On a essayé de voir quels étaient les endroits où il se produisaient le plus d'accidents


Les résultats de notre analyse se trouve dans les fichiers notes











