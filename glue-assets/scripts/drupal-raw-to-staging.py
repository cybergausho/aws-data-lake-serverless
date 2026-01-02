import sys
import boto3
import json
import re 
import html
import unicodedata
from bs4 import BeautifulSoup
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucketRaw"])
sc = SparkContext.getOrCreate()
spark = (
    SparkSession.builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ==============================================================================
# CONFIGURACION
# ==============================================================================
source = "drupal"
schema_Origen = "raw"
schema_Destino = "staging"
dbName = "tramites"

bucket_name = args['bucketRaw']
# Input: JSON Crudo
input_path = f"s3://{bucket_name}/{source}/{schema_Origen}/{dbName}/"
# Output: Staging (Parquet Planos)
output_base_path = f"{source}/{schema_Destino}/" 
# Ruta completa para Spark (Escritura): "s3://bucket/drupal/staging/"
output_full_uri = f"s3://{bucket_name}/{output_base_path}"

# ==============================================================================
# DEFINICIÓN DE UDFS
# ==============================================================================

def clean_html(raw_html):
    if not raw_html : return None
    # Decodificar entidades
    cleantext = html.unescape(raw_html)
    # Eliminar tags HTML
    cleantext = re.sub(r'<[^>]+>', ' ', cleantext)
    # Reemplazar caracteres de control (\r\n\t) por espacios
    cleantext = re.sub(r'[\r\n\t]+', ' ', cleantext)
    # Normalizar espacios multiples a uno solo
    cleantext = re.sub(r'\s+', ' ', cleantext)
    return cleantext.strip()

def normalize_text(text):
    if not text: return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", text).strip().lower()

def extract_url_if_keyword(body_html, keyword):
    if not body_html or keyword not in body_html.lower(): return None
    link_match = re.search(r'<a\s+(?:[^>]*?\s+)?href="(https:[^"]*)"', body_html, re.IGNORECASE)
    return link_match.group(1) if link_match else None

def extract_participants_from_html(body_content):
    if not body_content: return []
    body_html = ""
    try:
        if body_content.strip().startswith('[') and body_content.strip().endswith(']'):
            body_list = json.loads(body_content)
            body_html = "".join(body_list)
        else: body_html = body_content
    except: body_html = str(body_content or "")
    # Lo hacemos 2 veces: para los literales y los reales
    body_html = body_html.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
    body_html = body_html.replace('\\r', ' ').replace('\\n', ' ').replace('\\t', ' ')
    
    li_contents = re.findall(r'<li>(.*?)<\/li>', body_html, re.DOTALL | re.IGNORECASE)
    participants = set()
    # Blacklist
    BLACKLIST = [
        "escritura", "acta", "permiso", "constancia", "informe", "certificado", 
        "partida", "plano", "reglamento", "contrato", "estatuto", "nota",
        "si el", "en caso", "cuando", "donde", "para el", "baldío", "obra nueva",
        "título", "boleta", "frente", "dorso"
    ]
    for content in li_contents:
        #LIMPIEZA DEL CONTENIDO
        cleaned_content = clean_html(content)
        if not cleaned_content: continue
        # Eliminar puntuación final y espacios remanentes
        # eliminar puntos finales
        cleaned_content = cleaned_content.strip(" .,;:-_")        
        # Separar roles multiples
        if ',' in cleaned_content and len(cleaned_content) < 60: 
            roles_raw = [r.strip() for r in cleaned_content.split(',')]
        else: 
            roles_raw = [cleaned_content]

        for role in roles_raw:
            # Filtros de Calidad
            role_norm = normalize_text(role)
            
            # Filtros de longitud y Blacklist
            if len(role) > 50 or len(role) < 3: continue
            if any(bad_word in role_norm for bad_word in BLACKLIST): continue

            # LIMPIEZA FINA DE CONECTORES
            role = re.sub(r'\s+(o|y|del|para|un|una|el|la|los|las|de|en|por)\s*$', '', role, flags=re.IGNORECASE).strip()
            role = re.sub(r'^\s*(o|y|del|para|un|una|el|la|los|las|de|en|por)\s+', '', role, flags=re.IGNORECASE).strip()
            # Último strip por si quedaron puntos después de borrar conectores
            role = role.strip(" .,;:")
            if role: 
                # Primera mayuscula
                role_capitalized = role[0].upper() + role[1:]
                participants.add(role_capitalized)

    # Agregamos defaults
    participants.add("Ciudadano / Particular")
    participants.add("Empresa / Organización")
    participants.add("Profesional Matriculado")
    return list(participants)

def extract_medios_pago_json(html):
    if not html: return []
    medios_dict = {
        "Pago Fácil": r"\bpago\s*facil\b", "Rapipago": r"\brapipago\b", "Banco Ciudad": r"\bbanco\s*ciudad\b",
        "Banco Provincia": r"\bbanco\s*provincia\b", "Mercado Pago": r"\bmercado\s*pago\b",
        "Pago mis cuentas": r"\bpago\s*mis\s*cuentas\b", "Bapro Pagos": r"\bbapro\s*pagos\b",
        "Provincia Net": r"\bprovincia\s*net\b", "Visa": r"\bvisa\b", "MasterCard": r"\bmaster\s*card\b",
        "Cabal": r"\bcabal\b", "American Express": r"\bamerican\s*express\b", "ATM": r"\batm[s]?\b",
        "Página web": r"\bpagina\s*web\b", "BUEPP": r"\bbuepp\b",
    }
    encontrados = set()
    def limpiar_html_y_buscar(texto):
        if not texto: return
        plain = BeautifulSoup(str(texto), "html.parser").get_text(" ")
        plain_norm = normalize_text(plain)
        for nombre, patron in medios_dict.items():
            if re.search(patron, plain_norm, flags=re.IGNORECASE): encontrados.add(nombre)
    def recorrer(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in ["body", "link", "field_body", "field_texto", "descripcion"]: limpiar_html_y_buscar(v)
                else: recorrer(v)
        elif isinstance(obj, list):
            for i in obj: recorrer(i)
        elif isinstance(obj, str): limpiar_html_y_buscar(obj)
    recorrer(html)
    return sorted(encontrados)

# UDF PLATAFORMA
PLATFORM_SCHEMA = StructType([
    StructField("NOMBRE_PLATAFORMA", StringType(), True),
    StructField("LINK_PLATAFORMA", StringType(), True)
])
def extract_platform_from_step(body_html):
    if not body_html: return (None, None)
    PLATAFORMAS = {
        "TAD": r"tad\.buenosaires\.gob\.ar", "SIGECI": r"formulario-sigeci\.buenosaires\.gob\.ar",
        "AGControl": r"atencionvirtual\.agcontrol\.gob\.ar", "miBA": r"login\.buenosaires\.gob\.ar/auth",
        "AGIP": r"agip\.gob\.ar", "AFIP": r"afip\.gob\.ar"
    }
    soup = BeautifulSoup(body_html, "html.parser")
    link_tag = soup.find('a', href=True)
    if not link_tag: return (None, None)
    link = link_tag['href']
    for nombre, patron in PLATAFORMAS.items():
        if re.search(patron, link, re.IGNORECASE): return (nombre, link)
    if "mapa.buenosaires.gob.ar" in link: return (None, None)
    if re.search(r'\.(pdf|zip|doc|docx|jpg|png|dwg)$', link, re.IGNORECASE): return (None, None)
    if link.startswith("http"): return ("Plataforma Web", link)
    return (None, None)

def extract_miba_level(json_string):
    if not json_string: return None
    found_levels = re.findall(r'nivel\s+(\d)', json_string, re.IGNORECASE)
    return max([int(d) for d in found_levels]) if found_levels else None

# UDF DOCUMENTACION
DOCUMENTATION_SCHEMA = ArrayType(StructType([
    StructField("ID_DOCUMENTO", StringType(), True),
    StructField("NOMBRE_DOCUMENTO", StringType(), True),
    StructField("DESCRIPCION_DOCUMENTO", StringType(), True),
    StructField("DURACION_VALIDEZ", IntegerType(), True),
    StructField("TIENE_MODELO", StringType(), True),
    StructField("LINK_MODELO", StringType(), True),
    StructField("TIPO_DOCUMENTO", StringType(), True) 
]))

def clean_doc_title(text):
    if not text: return None
    t = clean_html(text)
    # Quitar caracteres de puntuación iniciales (ej: , donde conste...)
    t = re.sub(r'^[\s,.-]+', '', t)    
    # Quitar conectores iniciales comunes (ej: y copia, del titular)
    t = re.sub(r'^(y|o|e|u|donde|que|el|la|los|las|un|una|del|de|por|para|con)\s+', '', t, flags=re.IGNORECASE)
    t = t.strip()
    # Si quedo vacio o muy corto (menos 3 letras), descartar
    if len(t) < 3: return None    
    return t[0].upper() + t[1:]

def extract_documentation_info(body_content):
    documents = []
    OBLIGATORY_RE = re.compile(r'Documentación obligatoria', re.IGNORECASE)
    OPTIONAL_RE = re.compile(r'Documentación opcional', re.IGNORECASE)
    GENERIC_RE = re.compile(r'(Documentación a presentar|Documentos requeridos|Documentación requerida|Documentos a adjuntar)', re.IGNORECASE)
    
    # LISTA ANTI-GENERICOS
    GENERIC_LINK_TEXTS = [
        "AQUÍ", "AQUI", "ACA", "ACÁ", "DESCARGAR", "DESCARGA", "VER", 
        "LINK", "CLIC", "CLICK", "INGRESANDO", "MODELO", "PLANILLA", "SITIO WEB"
    ]

    full_body = ""
    try:
        if body_content:
            if isinstance(body_content, list): full_body = " ".join([str(x) for x in body_content])
            elif str(body_content).strip().startswith('['): full_body = " ".join(json.loads(str(body_content)))
            else: full_body = str(body_content)
    except: full_body = str(body_content or "")
    
    soup = BeautifulSoup(full_body, 'html.parser')
    found_docs = False
    
    def extract_list_items(ul_tag, doc_type):
        if not ul_tag: return
        for li in ul_tag.find_all('li'):
            # Trabajamos sobre una copia para no romper el HTML original al extraer
            temp_li = BeautifulSoup(str(li), 'html.parser')
            
            # Buscamos si hay link par el Modelo
            link_tag = temp_li.find('a', href=True)
            link_url = link_tag['href'] if link_tag else None
            tiene_modelo = 'Y' if link_url else 'N'
            
            nombre_final = None
            desc_final = ""
            # ESTRATEGIA A: HAY LINK
            if link_tag:
                link_text = clean_html(link_tag.get_text())
                link_tag.decompose() 
                context_text = clean_html(temp_li.get_text())
                
                # Chequeo de genéricos
                es_generico = link_text.upper() in GENERIC_LINK_TEXTS
                es_muy_corto = len(link_text) < 4
                
                if es_generico or es_muy_corto:
                    nombre_final = context_text 
                    desc_final = ""
                else:
                    nombre_final = link_text
                    desc_final = context_text
            else:
                full_text = clean_html(temp_li.get_text())
                if full_text and ':' in full_text:
                    parts = full_text.split(':', 1)
                    nombre_final = parts[0]
                    desc_final = parts[1]
                else:
                    nombre_final = full_text
                    desc_final = ""
            # LIMPIEZA FINAL
            if nombre_final:
                # Usamos la funcion clean_doc_title para normalizar el nombre
                nombre_clean = clean_doc_title(nombre_final)                
                # Limpiamos la descrip
                desc_clean = desc_final.strip(" .,;:-_()") if desc_final else ""
                if not nombre_clean and desc_final:
                     nombre_clean = clean_doc_title(desc_final)
                     desc_clean = ""
                if nombre_clean:
                    documents.append({
                        "ID_DOCUMENTO": None, 
                        "NOMBRE_DOCUMENTO": nombre_clean, 
                        "DESCRIPCION_DOCUMENTO": desc_clean,
                        "DURACION_VALIDEZ": None, 
                        "TIENE_MODELO": tiene_modelo, 
                        "LINK_MODELO": link_url, 
                        "TIPO_DOCUMENTO": doc_type 
                    })
    
    obligatory_header = soup.find(re.compile('h[1-4]'), string=OBLIGATORY_RE)
    if obligatory_header:
        ul_list = obligatory_header.find_next(['ul', 'ol'])
        if ul_list: extract_list_items(ul_list, "Obligatoria"); found_docs = True

    optional_header = soup.find(re.compile('h[1-4]'), string=OPTIONAL_RE)
    if optional_header:
        next_tag = optional_header.find_next(['ul', 'ol', 'p'])
        if next_tag:
            if next_tag.name in ['ul', 'ol']: extract_list_items(next_tag, "Opcional")
            elif next_tag.name == 'p':
                fake_ul = BeautifulSoup(f"<ul><li>{next_tag.decode_contents()}</li></ul>", "html.parser")
                extract_list_items(fake_ul.ul, "Opcional")
            found_docs = True

    if not found_docs:
        generic_text_match = soup.find(string=GENERIC_RE)
        if generic_text_match and generic_text_match.parent:
            ul_list = generic_text_match.parent.find_next(['ul', 'ol'])
            if ul_list: extract_list_items(ul_list, "Obligatoria")
    return documents

# ==============================================================================
# LOGICA DE REQUISITOS
# ==============================================================================

ReqDetailSchema = ArrayType(StructType([
    StructField("NOMBRE_REQUISITO", StringType(), True),
    StructField("OBLIGATORIEDAD", StringType(), True),
    StructField("PERSONA_FISICA", StringType(), True),
    StructField("ID_SUBGRUPO", StringType(), True)
]))

def extract_requirements_exploded(titulo_bloque, subtitulo_bloque, body_content):
    """
    Parsea recursivamente las listas HTML (<ul><li>).
    Concatena padres e hijos: "Persona Juridica: Estatuto".
    """
    reqs_found = []
    full_body = ""
    try:
        if body_content:
            if isinstance(body_content, list):
                full_body = " ".join([str(x) for x in body_content])
            elif isinstance(body_content, str):
                if body_content.strip().startswith('['):
                    try: full_body = " ".join(json.loads(body_content))
                    except: full_body = body_content
                else: full_body = body_content
    except: full_body = str(body_content or "")
    if not full_body: return []
    soup = BeautifulSoup(full_body, 'html.parser')    
    # persona fisica/juridica
    def detect_persona_local(text):
        t = normalize_text(text)
        if "juridica" in t or "empresa" in t or "sociedad" in t or "apoderado" in t: return "N"
        if "fisica" in t or "ciudadano" in t or "persona" in t: return "Y"
        return "Y" # Default
    # Funcion recursiva
    def parse_ul(ul_tag, parent_context=""):
        if not ul_tag: return
        
        for li in ul_tag.find_all('li', recursive=False):
            # Texto propio del li
            own_text_parts = []
            for child in li.children:
                if child.name != 'ul':
                    own_text_parts.append(child.get_text(strip=True))
            
            raw_text = " ".join(own_text_parts)
            clean_text = clean_html(raw_text)
            
            # Construimos contexto
            current_name = f"{parent_context}: {clean_text}" if parent_context and clean_text else clean_text
            current_name = re.sub(r':\s*$', '', current_name).strip()
            current_name = re.sub(r'^:\s*', '', current_name).strip()

            child_ul = li.find('ul')
            
            if child_ul:
                # Profundizamos
                next_context = current_name if current_name else parent_context
                parse_ul(child_ul, next_context)
            else:
                if current_name and len(current_name) > 3:
                    norm_name = normalize_text(current_name)
                    obligatorio = "N" if ("opcional" in norm_name or "si corresponde" in norm_name) else "Y"
                    if subtitulo_bloque and "opcional" in normalize_text(subtitulo_bloque):
                        obligatorio = "N"
                    p_fisica = detect_persona_local(current_name)
                    
                    reqs_found.append({
                        "NOMBRE_REQUISITO": current_name[0].upper() + current_name[1:],
                        "OBLIGATORIEDAD": obligatorio,
                        "PERSONA_FISICA": p_fisica,
                        "ID_SUBGRUPO": "GENERAL"
                    })

    # Inicio del parsing
    found_root = False
    for ul in soup.find_all('ul'):
        if ul.find_parent('ul') is None:
            parse_ul(ul, "")
            found_root = True
            
    if not found_root:
        # Fallback a parrafos
        for p in soup.find_all('p'):
            t = clean_html(p.get_text())
            if t and len(t) > 5 and "miba" not in normalize_text(t):
                 reqs_found.append({
                    "NOMBRE_REQUISITO": t,
                    "OBLIGATORIEDAD": "Y",
                    "PERSONA_FISICA": "Y", 
                    "ID_SUBGRUPO": "GENERAL"
                })

    return reqs_found


# UDF ORGANISMOS
OrganismoDetailSchema = StructType([
    StructField("NOMBRE_DEPENDENCIA", StringType(), True), StructField("DESCRIPCION_DEPENDENCIA", StringType(), True),
    StructField("HORARIO_DEPENDENCIA", StringType(), True), StructField("DIRECCION_DEPENDENCIA", StringType(), True),
    StructField("TELEFONO_DEPENDENCIA", StringType(), True), StructField("MAIL_ATENCION", StringType(), True),
    StructField("DEPENDENCIA_MINISTERIO", StringType(), True)
])
def extract_organismo_detail(body_html):
    if not body_html: return (None, None, None, None, None, None, None)
    soup = BeautifulSoup(body_html, "html.parser")
    text_para_contacto = soup.get_text()
    
    nombre = None
    org_block = soup.find(lambda tag: tag.name in ["h2", "h3", "h4"] and "organismo" in tag.get_text(strip=True).lower())
    if org_block:
        next_tag = org_block.find_next(lambda tag: tag.name in ["h2", "h3", "h4", "p"] and tag.get_text(strip=True))
        if next_tag: nombre = clean_html(next_tag.get_text()); next_tag.decompose()
        org_block.decompose()
    
    direccion = None
    dir_block = soup.find(lambda tag: "ubicación" in tag.get_text(strip=True).lower() or "dirección" in tag.get_text(strip=True).lower())
    if dir_block:
        direccion_parts = []
        next_tag = dir_block.find_next_sibling()
        while next_tag and next_tag.name == 'p':
            part = clean_html(next_tag.get_text())
            if part: direccion_parts.append(part)
            tag_to_remove = next_tag; next_tag = next_tag.find_next_sibling(); tag_to_remove.decompose()
        if direccion_parts: direccion = " ".join(direccion_parts)
        else:
            direccion_raw = clean_html(dir_block.get_text())
            direccion = re.sub(r'^(Dirección|Ubicación)', '', direccion_raw, flags=re.IGNORECASE).strip()
        dir_block.decompose()
    if not direccion:
        for p in soup.find_all("p"):
            p_text = p.get_text()
            if re.search(r'((Av\.|Avenida|Calle|Cnel\.)\s|Uspallata\s|Bolívar\s|Lima\s|Viamonte\s|Defensa\s|Piedra\sBuena|Corrientes\s)', p_text, re.IGNORECASE) and re.search(r'\d{2,}', p_text):
                direccion = clean_html(p_text); p.decompose(); break
    
    horario_final = None
    horario_parts_total = []
    horario_blocks = soup.find_all(lambda tag: tag.name in ["h2", "h3", "h4"] and ("horario" in tag.get_text(strip=True).lower() or "días de atención" in tag.get_text(strip=True).lower()))
    if horario_blocks:
        for horario_block in horario_blocks:
            horario_parts_individual = []
            next_tag = horario_block.find_next_sibling()
            while next_tag and next_tag.name == 'p':
                part = clean_html(next_tag.get_text())
                if part: horario_parts_individual.append(part)
                tag_to_remove = next_tag; next_tag = next_tag.find_next_sibling(); tag_to_remove.decompose()
            if horario_parts_individual: horario_parts_total.extend(horario_parts_individual)
            else:
                horario_raw = clean_html(horario_block.get_text())
                horario_clean = re.sub(r'^(Días de atención|Horarios de atención|Horario)', '', horario_raw, flags=re.IGNORECASE).strip()
                if horario_clean: horario_parts_total.append(horario_clean)
            horario_block.decompose()
    
    text_para_horario_fallback = soup.get_text(separator="\n")
    if not horario_parts_total:
        match = re.search(r'(D[ií]as?.*(horario[s]?|atenci[oó]n).*?:?\s*)([^\n<]+)', text_para_horario_fallback, re.IGNORECASE)
        if match:
            horario_final = match.group(2).strip()
            tag_horario = soup.find(lambda tag: match.group(0) in tag.get_text())
            if tag_horario: tag_horario.decompose()
    if horario_parts_total: horario_final = " ".join(horario_parts_total)
    horario_final = horario_final or "No especificado"

    telefono = None
    mail_atencion = None
    tel_match = re.search(r'(?:(?:Tel[eé]fono|Contacto|llamar(?: al)?|comunicate(?: al)?)\D{0,40}?)(\d{4,5}[- ]?\d{3,4})', text_para_contacto, re.IGNORECASE)
    if tel_match: telefono = tel_match.group(1).replace(" ", "")
    mail_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text_para_contacto, re.IGNORECASE)
    if mail_match: mail_atencion = mail_match.group(0)

    ministerio = nombre if nombre and "ministerio" in nombre.lower() else None
    descripcion_limpia = clean_html(soup.get_text(separator=' ')).strip() or None
    return (nombre, descripcion_limpia, horario_final, direccion, telefono, mail_atencion, ministerio)

# ==============================================================================
# REGISTRO UDFS
# ==============================================================================
extract_req_exploded_udf = F.udf(extract_requirements_exploded, ReqDetailSchema)
clean_html_udf = F.udf(clean_html, StringType())
extract_turnos_url_udf = F.udf(lambda body: extract_url_if_keyword(body, "turno"), StringType())
extract_participants_udf = F.udf(extract_participants_from_html, ArrayType(StringType()))
extract_medio_pago_udf = F.udf(extract_medios_pago_json, ArrayType(StringType()))
extract_platform_udf = F.udf(extract_platform_from_step, PLATFORM_SCHEMA)
extract_miba_level_udf = F.udf(extract_miba_level, IntegerType())
extract_documentation_udf_spark = F.udf(extract_documentation_info, DOCUMENTATION_SCHEMA)
extract_organismo_udf = F.udf(extract_organismo_detail, OrganismoDetailSchema)

# ==============================================================================
# LECTURA
# ==============================================================================
schema = StructType([
    StructField("id", StringType(), True), 
    StructField("type", StringType(), True),
    StructField("title", StringType(), True), 
    StructField("created", StringType(), True),
    StructField("changed", StringType(), True), 
    StructField("url", StringType(), True),
    StructField("fields", StructType([
        StructField("body", StringType(), True), 
        StructField("title", StringType(), True),
        StructField("langcode", StringType(), True),
        StructField("field_areas", StructType([StructField("id", StringType(), True), StructField("title", StringType(), True), StructField("url", StringType(), True)]), True),
        StructField("field_organismos", StructType([StructField("id", StringType(), True), StructField("fields", StructType([StructField("body", StringType(), True)]), True)]), True),
        StructField("field_tramite_modalidad", ArrayType(StructType([StructField("target_id", StringType(), True)])), True),
        StructField("field_content", ArrayType(StructType([
            StructField("id", StringType(), True), 
            StructField("type", StringType(), True),
            StructField("fields", StructType([
                StructField("title", StringType(), True),
                StructField("body", ArrayType(StringType()), True), 
                StructField("field_subtitulo", StringType(), True),
                
                StructField("items", StructType([
                    StructField("id", StringType(), True), 
                    StructField("type", StringType(), True), 
                    StructField("fields", StructType([
                        StructField("field_name", StringType(), True), 
                        StructField("field_price", StringType(), True)
                    ]), True)
                ]), True),
                StructField("field_items_paso_tramite", ArrayType(StructType([StructField("id", StringType(), True), StructField("type", StringType(), True), StructField("fields", StructType([StructField("title", StringType(), True), StructField("body", StringType(), True)]), True)])), True),
            ]), True)
        ])), True)
    ]), True)
])

df = spark.read.schema(schema).json(input_path, multiLine=True)
df_exploded = df.withColumn("content", F.explode_outer(F.col("fields.field_content")))

# ==============================================================================
# TRANSFORMACIONES
# ==============================================================================
df_req_raw = df_exploded.filter(F.col("content.type") == "requisito_tramite")
df_reqs_staging = df_req_raw.select(
    F.col("id").alias("ID_TRAMITE"),
    F.col("content.id").alias("ID_CONTENT_RAW"),
    extract_req_exploded_udf(
        F.col("content.fields.title"), 
        F.col("content.fields.field_subtitulo"),
        F.col("content.fields.body")
    ).alias("REQS_PARSED")
).select("ID_TRAMITE", "ID_CONTENT_RAW", F.explode_outer("REQS_PARSED").alias("r")).select(
    "ID_TRAMITE", "ID_CONTENT_RAW", F.col("r.NOMBRE_REQUISITO"), F.col("r.OBLIGATORIEDAD"), F.col("r.PERSONA_FISICA"), F.col("r.ID_SUBGRUPO")
).filter(F.col("NOMBRE_REQUISITO").isNotNull())

# TRAMITES BASE
df_base_staging = df.select(
    F.col("id").alias("CODIGO_TRAMITE"), 
    F.col("title").alias("NOMBRE"),
    F.col("created"), F.col("changed"),
    clean_html_udf(F.col("fields.body")).alias("FINALIDAD"),
    F.col("fields.field_organismos.id").alias("ID_DEPENDENCIA"),
    F.lit("1").alias("ID_TIPO_TRAMITE"),
    extract_miba_level_udf(F.to_json(F.struct(F.col("*")))).alias("NIVEL_MIBA"),
    extract_medio_pago_udf(F.to_json(F.struct(F.col("*")))).alias("MEDIOS_PAGO_ARRAY"),
    F.col("fields.field_tramite_modalidad.target_id").alias("MODALIDAD_IDS")
)

# DOCUMENTOS
df_docs_staging = df_exploded.filter(F.col("content.type") == "requisito_tramite").withColumn(
    "docs_array", extract_documentation_udf_spark(F.col("content.fields.body"))
).select(
    F.col("id").alias("ID_TRAMITE"), 
    F.col("content.id").alias("ID_CONTENT_RAW"),
    F.explode_outer("docs_array").alias("doc")
).select("ID_TRAMITE", "ID_CONTENT_RAW", "doc.*").filter(F.col("NOMBRE_DOCUMENTO").isNotNull()
).withColumn("DURACION_VALIDEZ", F.when(F.col("DURACION_VALIDEZ") == 0, F.lit(None)).otherwise(F.col("DURACION_VALIDEZ")).cast(IntegerType()))
# PASOS
df_pasos_staging = df_exploded.filter(F.col("content.type") == "pasos_del_tramite").select(
    F.col("id").alias("ID_TRAMITE"),
    F.posexplode_outer("content.fields.field_items_paso_tramite").alias("ORDEN_PASO", "paso")
).filter(F.col("paso").isNotNull()).select(
    "ID_TRAMITE",
    (F.col("ORDEN_PASO") + 1).alias("ORDEN_PASO"), 
    F.col("paso.fields.title").alias("TITULO_PASO_RAW"),
    clean_html_udf(F.col("paso.fields.body")).alias("DESCRIPCION_PASO_CLEAN"),
    F.col("paso.fields.body").alias("DESCRIPCION_PASO_HTML")
).withColumn("plataforma_info", extract_platform_udf(F.col("DESCRIPCION_PASO_HTML"))) \
 .withColumn("CANAL_TURNO", extract_turnos_url_udf(F.col("DESCRIPCION_PASO_HTML"))) \
 .select("*", "plataforma_info.*") \
 .withColumn("INTERVINIENTE", F.lit(None).cast(StringType())) \
 .withColumn("MOSTRAR_EN_PLATAFORMA", F.lit("S")) # Default
# PARTICIPANTES
df_part_staging = df_exploded.filter(F.col("content.fields.field_subtitulo") == "Participantes y Profesionales responsables").select(
    F.col("id").alias("ID_TRAMITE"), extract_participants_udf(F.col("content.fields.body")).alias("ROLES_ARRAY")
).select("ID_TRAMITE", F.explode_outer("ROLES_ARRAY").alias("NOMBRE_PARTICIPANTE"))

# COSTOS
df_costos_staging = df_exploded.filter(F.col("content.type") == "costo_del_tramite").select(
    F.col("id").alias("ID_TRAMITE"),
    F.col("content.fields.items.fields.field_name").alias("CONCEPTO"),
    F.col("content.fields.items.fields.field_price").alias("PRECIO_STR")
)

# DEPENDENCIAS
df_deps_staging = df.select(
    F.col("fields.field_organismos.id").alias("ID_DEPENDENCIA"),
    extract_organismo_udf(F.col("fields.field_organismos.fields.body")).alias("det")
).select("ID_DEPENDENCIA", "det.*").dropDuplicates(["ID_DEPENDENCIA"])

# MARCO NORMATIVO (FILTRADO POR TITULO)
df_marco_staging = df_exploded.filter(
    (F.col("content.type") == "paragraph") & 
    (F.lower(F.col("content.fields.title")).contains("normativa"))
).select(
    F.col("id").alias("ID_TRAMITE"),
    clean_html_udf(F.col("content.fields.body")).alias("BODY_CLEAN")
).filter(F.col("BODY_CLEAN").isNotNull())

# TIPO TRAMITE
df_tipo_staging = df.select(
    F.col("id").alias("ID_TIPO_TRAMITE"), F.col("title").alias("TITULO_TIPO_TRAMITE"),
    clean_html_udf(F.col("fields.body")).alias("DESCRIPCION_ESTADO")
).filter(F.col("ID_TIPO_TRAMITE").isNotNull())

# ==============================================================================
# ESCRITURA
# ==============================================================================
def purge_s3_folder(bucket, prefix):
    s3 = boto3.resource('s3')
    bucket_res = s3.Bucket(bucket)
    bucket_res.objects.filter(Prefix=prefix).delete()

# Lista de carpetas a purgar
carpetas = [
    "tramites_base", "documentos", "pasos", "requisitos_norm", 
    "participantes", "costos", "dependencias", "marco_normativo", "tipo_tramite"
]

print(f"Purgando carpetas en {output_full_uri}...")
for folder in carpetas:
    purge_s3_folder(bucket_name, f"{output_base_path}{folder}/")

def write_parquet(df, folder_name):
    df.write.mode("overwrite").parquet(f"{output_full_uri}{folder_name}/")

write_parquet(df_base_staging, "tramites_base")
write_parquet(df_docs_staging, "documentos")
write_parquet(df_pasos_staging, "pasos")
write_parquet(df_reqs_staging, "requisitos_norm")
write_parquet(df_part_staging, "participantes")
write_parquet(df_costos_staging, "costos")
write_parquet(df_deps_staging, "dependencias")
write_parquet(df_marco_staging, "marco_normativo")
write_parquet(df_tipo_staging, "tipo_tramite")

job.commit()