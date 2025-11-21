## 🟩 2. Instalacja Narzędzi (Linux / WSL)

### 🔧 2.1 Java 11

Zainstaluj **OpenJDK 11** — wymagane JDK dla Sparka i sbt.

```bash
sudo apt update
sudo apt install openjdk-11-jdk -y
java -version
```

# Dodanie klucza GPG i repozytorium sbt
```bash
sudo wget -O /usr/share/keyrings/sbt-archive-keyring.gpg [https://repo.scala-sbt.org/scalasbt/debian/sbt-archive-keyring.gpg](https://repo.scala-sbt.org/scalasbt/debian/sbt-archive-keyring.gpg)
echo "deb [signed-by=/usr/share/keyrings/sbt-archive-keyring.gpg] [https://repo.scala-sbt.org/scalasbt/debian](https://repo.scala-sbt.org/scalasbt/debian) all main" | sudo tee /etc/apt/sources.list.d/sbt.list
```


# Aktualizacja i instalacja
```bash
sudo apt update
sudo apt install sbt -y
sbt --version
```


# Pobierz gotową paczkę Sparka
```bash
wget [https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz](https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz)
```


# Rozpakuj i przenieś do folderu domowego
```bash
tar -xvzf spark-3.5.0-bin-hadoop3.tgz
mv spark-3.5.0-bin-hadoop3 ~/spark
```

# Dodaj Spark do PATH (konfiguracja zmiennych środowiskowych)
```bash
echo 'export SPARK_HOME=~/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

# Test (uruchomienie powłoki Spark)
```bash
spark-shell
```

# Pobierz gotową paczkę Sparka
```bash
wget [https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz](https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz)
```

# Rozpakuj i przenieś do folderu domowego
```bash
tar -xvzf spark-3.5.0-bin-hadoop3.tgz
mv spark-3.5.0-bin-hadoop3 ~/spark
```

# Dodaj Spark do PATH (konfiguracja zmiennych środowiskowych)
```bash
echo 'export SPARK_HOME=~/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

# Test (uruchomienie powłoki Spark)
```bash
spark-shell
```

# Intelij odpalasz jako remote developemnt w wsl i zainstaluj plugin scali i sbt jezeli odpalsiz noramnie nie przez remote developemnt to nie zadziala