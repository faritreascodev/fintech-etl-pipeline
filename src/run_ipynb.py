import json
import os

# Abrir el notebook en formato JSON
with open("src/fintech_analytics.ipynb", "r", encoding="utf-8") as f:
    nb = json.load(f)

# Filtrar solo las celdas de código
code_cells = [cell for cell in nb["cells"] if cell["cell_type"] == "code"]

# Unir todo el código en un solo string
code = ""
for cell in code_cells:
    source = "".join(cell["source"])
    code += source + "\n\n"

# Reemplazar rutas específicas de Colab por rutas locales
code = code.replace("/content/out_es", "data/out_es")

# Configuración extra para simular o ignorar display() de IPython
import builtins

def mock_display(*args, **kwargs):
    # Imprime los argumentos en consola en lugar de usar display
    for arg in args:
        print(arg)

# Sobrescribir display con la función mock
builtins.display = mock_display

# Ejecutar el bloque de código combinado
print("Ejecutando código extraído del notebook...")
try:
    exec(code, globals())
    print("¡Ejecución del notebook completada con éxito!")
except Exception as e:
    print(f"Error en la ejecución del notebook: {e}")
    import traceback
    traceback.print_exc()