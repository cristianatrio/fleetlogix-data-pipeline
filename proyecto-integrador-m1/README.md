# Proyecto Integrador M1 – Conociendo al Cliente 360°

Análisis del mercado gastronómico en EE. UU. · Data Science – SoyHenry · **Cristian Atrio**

Análisis del comportamiento de clientes de una empresa de marketing gastronómico, integrando la base interna de clientes con información externa de restaurantes obtenida desde la **API de Yelp Fusion**. El objetivo es convertir datos crudos en insights accionables para segmentar clientes, optimizar campañas y detectar oportunidades de expansión.

## Contenido

```
proyecto-integrador-m1/
├── Avances/
│   ├── Avance_1_EDA.ipynb                    # Limpieza y análisis exploratorio
│   ├── Avance_2_API_Yelp.ipynb               # Extracción e integración con la API de Yelp
│   ├── base_datos_restaurantes_USA_v2.csv    # Dataset principal (30.000 clientes)
│   ├── datos_reales_yelp.csv                 # Resultado crudo de la API
│   └── chicago_restaurants_integrated.csv    # Dataset integrado y transformado
├── Documentacion/
│   ├── readme.docx                           # Documentación del proyecto
│   └── Recomendaciones.pdf                   # Recomendaciones estratégicas
└── .env.example                              # Plantilla de variables de entorno
```

## Avances

### Avance 1 – EDA
- Carga del dataset de 30.000 clientes y revisión de su estructura.
- Limpieza: eliminación de duplicados, filtrado de edades inválidas (18–100), `"Sin especificar"` en preferencias alimenticias y mediana en el gasto promedio.
- Selección de **Chicago** como ciudad de foco (mayor volumen de clientes).
- Análisis demográfico, perfil socioeconómico, hábitos de gasto y segmentación por preferencias.

### Avance 2 – API de Yelp
- Consulta a Yelp Fusion (`/v3/businesses/search`) para restaurantes de Chicago.
- Transformación del JSON a tabla, tratamiento de precios nulos (`N/A`) e identificador `full_id`.
- Exportación a `datos_reales_yelp.csv` y `chicago_restaurants_integrated.csv`, más un diccionario de datos.

## Principales hallazgos

- El **10 % de los clientes** concentra el mayor gasto (≈ USD 677 mensuales por cliente), principalmente de estratos medio y alto.
- Las categorías de mayor afinidad en clientes VIP son **carnes** y **vegetariano**.
- **Chicago** lidera en clientes y en membresías premium.

**Recomendaciones:** replicar el modelo de membresías premium de Chicago en Miami, priorizar la captación de restaurantes de carnes y vegetarianos premium, y dirigir campañas a clientes de ingresos medios y altos. Detalle completo en `Documentacion/Recomendaciones.pdf`.

## Tecnologías

Python 3 · pandas · numpy · matplotlib · seaborn · requests · openpyxl · python-dotenv · Jupyter Notebook · VS Code

## Cómo ejecutarlo

1. Instalá Jupyter y abrí los notebooks en orden (`Avance_1_EDA` → `Avance_2_API_Yelp`). El primer código del notebook 2 instala las librerías necesarias.
2. Para el Avance 2 necesitás una **API key de Yelp Fusion** (gratuita). Copiá la plantilla y completá tu clave:

   ```bash
   cp .env.example .env
   ```

   ```
   YELP_API_KEY=tu_clave_aqui
   ```

   El notebook la lee con `os.environ['YELP_API_KEY']`. El archivo `.env` está en `.gitignore` y nunca debe subirse al repositorio.
3. Los notebooks leen y escriben los CSV con rutas relativas: abrilos con `Avances/` como directorio de trabajo.

## Nota sobre los datos

`base_datos_restaurantes_USA_v2.csv` es un dataset **sintético** (los correos usan dominios `example.*`), sin información de personas reales.

## Autor

**Cristian Atrio** – [@cristianatrio](https://github.com/cristianatrio)
