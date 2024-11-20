# E-simulator

E-simulator is a project that simulates a Kafka-based producer-consumer architecture for handling and processing messages related to products and customers. The project uses Python and SQLite for data management, Kafka for message handling, and Docker Desktop for containerized Kafka and Zookeeper services.

---

## 🗂 Project Structure

### **Producer**
- `producer.py`: Generates data and sends messages to Kafka.
- `producer_sql.py`: Manages SQLite database operations to read and manipulate product data.
- `products.txt`: A file containing product data.
- `requirements.txt`: List of Python packages required for the producer component.

### **Consumers**
- `consumer1.py` to `consumer6.py`: Various consumers subscribing to Kafka topics to read and process messages. Each consumer may have a specific role or functionality.

### **Other Files**
- `kafka-command.txt`: Contains Kafka commands for creating topics and managing Kafka services.
- `uppgiften.txt`: Describes the project's purpose and tasks to be completed.
- `.gitignore`: Ignore rules for version control.
- `requirements.txt`: List of Python packages required for the entire project.

---

## 🚀 How to Run the Project

### 1. Preparations
1. Install the necessary Python packages with:
   ```
   pip install -r requirements.txt
   ```
2. Ensure Docker Desktop is installed and running on your system.
3. Ensure Kafka is properly configured using Docker. This project relies on Docker Compose to start and manage Kafka and Zookeeper containers.

Use the commands provided in `kafka-command.txt` to set up Kafka topics and start the Kafka brokers.
Make sure that your Kafka instance is properly configured and running to enable communication between the producer and consumers.

3. Start the Producer

The producer component generates simulated product data and sends messages to the Kafka topics. To start the producer:

Navigate to the Producer directory in your terminal.
Run the following command:

`python producer.py`

The producer will begin generating messages and pushing them to the Kafka topics specified in the configuration.

4. Start the Consumers
The consumer scripts are designed to read and process messages from the Kafka topics. Each consumer may handle data differently based on its implementation. To start the consumers:

Open separate terminal windows for each consumer you wish to run.
Run each consumer script individually, for example:

```
python consumer1.py
python consumer2.py
python consumer3.py
```

Each consumer will subscribe to the relevant Kafka topics and process incoming messages in real-time.

### 🛠 Tools and Technologies
__Kafka__: Provides the messaging system to connect producers and consumers.

__Python__: Handles the core logic for data generation, database operations, and message consumption.

__SQLite__: A lightweight database used for storing and retrieving product data.

__Docker Desktop__: Manages containerized services for Kafka and Zookeeper using Docker Compose.

### 📄 Task
The project simulates a system for managing customer and product interactions, utilizing Kafka for real-time data flow and Python for logic processing. Specific project objectives and details can be found in the `uppgiften.txt` file, which describes the tasks implemented and expected outcomes.

### 📝 Dependencies
All necessary Python packages are listed in requirements.txt. Key libraries include:

-`kafka-python`

-`sqlite3`

-`Docker Desktop`

- Other packages required for data processing and Kafka integration.
Install all dependencies with:

`pip install -r requirements.txt`

### 🏗 Future Development
Add more consumer roles (if needed).
Improve producer logic with more advanced data flows.
Integrate with a real-time dashboard to display Kafka events.

### 🏃‍♂️ Execution Example
Here’s an example of how the project runs:

1. The producer generates product data and sends it to Kafka.
2. Consumers subscribe to Kafka topics and process the data in real-time.
3. Docker Desktop is used to manage Kafka and Zookeeper containers, simplifying service deployment.

All logs and results are displayed in real-time.

### 📂 License
This project is intended for internal or educational purposes. Feel free!