![HenryLogo](https://d31uz8lwfmyn8g.cloudfront.net/Assets/logo-henry-white-lg.png)

## Homework

1. Crear una tabla que permita realizar el seguimiento de los usuarios que ingresan nuevos registros en fact_venta.
2. Crear una acción que permita la carga de datos en la tabla anterior.
3. Crear una tabla que permita registrar la cantidad total registros, luego de cada ingreso la tabla fact_venta.
4. Crear una acción que permita la carga de datos en la tabla anterior.
5. Crear una tabla que agrupe los datos de la tabla del item 3, a su vez crear un proceso de carga de los datos agrupados.
6. Crear una tabla que permita realizar el seguimiento de la actualización de registros de la tabla fact_venta.
7. Crear una acción que permita la carga de datos en la tabla anterior, para su actualización.

### Carga Incremental

En este punto, ya contamos con toda la información de los datasets originales cargada en el DataWarehouse diseñado. Sin embargo, la gerencia, requiere la posibilidad de evaluar agregar a esa información la operatoria diara comenzando por la información de Ventas. Para lo cual, en la carpeta "Carga_Incremental" se disponibilizaron los archivos:

* Ventas_Actualizado.csv: Tiene una estructura idéntica al original, pero con los registros del día 01/01/2021.
* Clientes_Actializado.csv: Tiena una estructura idéntica al original, pero actualizado al día 01/01/2021.

Es necesario diseñar un proceso que permita ingestar al DataWarehouse las novedades diarias, tanto de la tabla de ventas como de la tabla de clientes.
Se debe tener en cuenta, que dichas fuentes actualizadas, contienen la información original más las novedades, por lo tanto, en la tabla de "ventas" es necesario identificar qué registros son nuevos y cuales ya estaban cargados anteriormente, y en la tabla de clientes tambien, con el agregado de que en ésta última, pueden haber además registros modificados, con lo que hay que hacer uso de los campos de auditoría de esa tabla, por ejemplo, Fecha_Modificación.