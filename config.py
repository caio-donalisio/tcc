
#PATHS
ORIGINAL_FILES_DIR = './01_original_files/'  # Arquivos originais <--- INSERIR PDFS AQUI
DESKEWED_FILES_DIR = './02_deskewed_files/' # Arquivos desinclinados
OCRED_PAGES_DIR = './03_ocred_pages_files/' # Arquivos com texto extraído pelo Google Vision
JOINED_OCRED_DIR = './04_joined_ocred_files/' # Arquivos resultados do OCR consolidados em um único arquivo
DEBUG_GRID_FILES_DIR = './05_debug_grid_files/' # Arquivos com grids desenhados
OUTPUT_TABLES_FILES_DIR = './06_output_table_files/' #Arquivos com tabelas extraídas ---> RESULTADO FINAL

#OPERATIONS
DESKEW=False
OCR=False
JOIN=False
GRID=False

#ROW DETECTION
GAP_BETWEEN_LINES = 0.007

#COLUMN DETECTION
TABLE_RESIZE = 800
CROPPED_RESIZE = 1000
CROP_PADDING = 0
NORMALIZE_VECTORS = ([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
DETECTION_CLASS_THRESHOLDS = {
    "table": 0.5,
    "table rotated": 0.5,
    "no object": 10
}
SIMILARITY_THRESHOLD = 7
SCORE_THRESHOLD = 0.7

BUCKET_NAME='tcc-caio-donalisio-93'