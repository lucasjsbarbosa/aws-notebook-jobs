
import subprocess, sys

# Certifique-se de ter papermill instalado
# subprocess.run([sys.executable, "-m", "pip", "install", "papermill"], check=True)

subprocess.run([
    "papermill",
    "/opt/ml/processing/input/04_pipeline_principal.ipynb",
    "/opt/ml/processing/output/04_pipeline_principal_executed.ipynb"
], check=True)

print("Notebook principal executado com sucesso!")
