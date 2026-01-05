# Preprocessing Module

This module contains Spark jobs written in Scala for preprocessing data from various sources. The jobs are packaged into a single "fat" JAR file, which is then used by Airflow DAGs to execute the preprocessing tasks on a Spark cluster.

## 1. Prerequisites

Before you can build and run the preprocessing jobs, you need to have the following installed on your local machine or development environment:

- **Java Development Kit (JDK):** Version 11 or 17 is recommended. This project has been tested with OpenJDK 17.
- **sbt (Simple Build Tool):** The official build tool for Scala projects.

### 1.1. Java Installation (OpenJDK 17 on Debian/Ubuntu)

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk
java -version
```

### 1.2. sbt Installation (on Debian/Ubuntu)

```bash
# Add the sbt repository and GPG key
echo "deb [signed-by=/usr/share/keyrings/sbt-keyring.gpg] https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo gpg --dearmor -o /usr/share/keyrings/sbt-keyring.gpg

# Update package list and install sbt
sudo apt-get update
sudo apt-get install -y sbt
sbt --version
```

## 2. Building the Project

To compile the Scala code and package it into a self-contained JAR file (an "assembly" or "fat JAR"), run the following command from the `preproccessing-module` directory:

```bash
sbt assembly
```

This command will:
1.  Download all the necessary dependencies.
2.  Compile your Scala source code.
3.  Package the compiled code and all its dependencies into a single JAR file.

The resulting JAR will be located at `target/scala-2.12/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar`.

### Troubleshooting

- **Permission Errors:** If you encounter permission errors during the build process, you may need to change the ownership of the project directory to your current user:
  ```bash
  sudo chown -R $(whoami) .
  ```

## 3. Deploying the JAR to Spark

The Airflow DAGs are configured to execute Spark jobs using a JAR file located in the `spark-master` container. After building the JAR, you need to copy it into the running container.

Use the following command from the root of the `preproccessing-module` directory:

```bash
docker cp target/scala-2.12/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar spark-master:/spark/jars/
```

After this step, your updated Spark jobs will be available to be triggered by the Airflow DAGs.
