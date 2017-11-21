Add-Type -AssemblyName System.IO.Compression.FileSystem

[System.IO.Compression.ZipFile]::ExtractToDirectory("C:\tmp\data-generator.zip", "C:\Users\Administrator\Desktop\data-generator\")
[System.IO.Compression.ZipFile]::ExtractToDirectory("C:\tmp\heatmap-generator.zip", "C:\Users\Administrator\Desktop\heatmap-generator\")

pip install --upgrade -r C:\Users\Administrator\Desktop\data-generator\requirements.txt
pip install --upgrade -r C:\Users\Administrator\Desktop\heatmap-generator\requirements.txt