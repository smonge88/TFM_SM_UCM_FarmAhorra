import subprocess
import sys
import os

#-------------------------------------------------------------------------------------------------------
# Este es el script principal para ejecutar todos los scripts en este orden:
# 1. python scripts/a_download_sets.py
# 2. python scripts/b_clean_nadac.py
# 3. python scripts/c_import_ndc_to_mongo.py
# 4. python scripts/d_clean_ndc.py
# 5. python scripts/e_normalize_package_ndc.py
# 6. python scripts/f_add_selling_size.py
# 7. python scripts/g_join_nadac_with_products.py
# 8. python scripts/h_generate_catalogs.py

# En resumen parte de la descarga de los archivos fuente de los websites
# y retorna el archivo products_enriched.csv en el directorio local y en MongoDB.
#-------------------------------------------------------------------------------------------------------


# Usa el mismo intérprete de Python que está ejecutando este script
PYTHON_EXEC = sys.executable

print(f"Usando Python: {sys.executable}")

scripts = [
    "scripts/a_download_sets.py",
    "scripts/b_clean_nadac.py",
    "scripts/c_import_ndc_to_mongo.py",
    "scripts/d_clean_ndc.py",
    "scripts/e_normalize_package_ndc.py",
    "scripts/f_add_selling_size.py",
    "scripts/g_join_nadac_with_products.py",
    "scripts/h_generate_catalogs.py",
]

# Función que corre todos los tests de las funciones usados en los scripts anteriores
# y que se encuentran dentro de la carpeta tests.
def run_tests():
    print("--------------------------------Ejecutando tests unitarios--------------------------------\n")
    result = subprocess.run([PYTHON_EXEC, "-m", "unittest", "discover", "-s", "tests"], cwd=os.getcwd())
    if result.returncode != 0:
        print("\nTests fallaron. Abortando ejecución del pipeline.")
        sys.exit(1)
    print("Todos los tests pasaron.\n")

# Función que permite ejecutar los scripts en orden
def run_script(script):
    print(f"\n--------------------------------Ejecutando: {script}------------------------------------------")
    result = subprocess.run(
        [PYTHON_EXEC, script],
        cwd=os.getcwd(),  # Usa el mismo directorio base
        check=False
    )
    if result.returncode != 0:
        print(f"Error al ejecutar {script}. Abortando.")
        sys.exit(1)

# Función main que organiza las dos funciones anteriores
def main():
    run_tests()
    print("Iniciando ejecución del pipeline\n")
    for script in scripts:
        run_script(script)
    print("\n--------------------------------Pipeline ejecutado exitosamente--------------------------------")

if __name__ == "__main__":
    main()

# correr script desde terminal con
# python scripts/z_run_pipeline.py

# para ejecutar todos los tests unitarios, correr desde la terminal:
# python -m unittest discover -s tests
