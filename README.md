# SYNERGY: Smart Home Energy Optimization System

Welcome to **SYNERGY** — a cutting-edge, data-driven smart home energy optimization platform that empowers you to reduce energy waste and live more sustainably. Harness the power of real-time IoT sensing, advanced machine learning, and intelligent recommendations to transform your home's energy efficiency.

## 🚀 Key Features

- **Real-Time Monitoring**
  - Track energy usage, occupancy, and environmental conditions (temperature, humidity, etc.) with a seamless network of IoT sensors.

- **Predictive Analytics**
  - **Energy Forecasting:** Anticipate future energy consumption for individual appliances.
  - **Peak Load Detection:** Identify high-consumption periods and receive actionable load balancing strategies.
  - **Anomaly Detection:** Instantly spot unusual energy patterns, such as idle devices drawing power.

- **AI-Powered Recommendations**
  - Get intelligent, context-aware suggestions to cut down energy consumption, powered by state-of-the-art language models.

- **Interactive Dashboard**
  - Enjoy a sleek, real-time web interface (built with Next.js) that visualizes energy data and actionable insights.
  - Multi-language support for global accessibility.
  - <img width="408" alt="Screenshot 2025-06-15 at 3 02 20 PM" src="https://github.com/user-attachments/assets/340e5349-d293-4dc2-9005-472ad3bddd54" />>


- **Smart Assistant Integration**
  - Effortlessly connect with Alexa, Google Home, and other popular smart assistants.

## 🧠 System Architecture

| Layer                | Technology Stack         | Description                                                        |
|----------------------|-------------------------|--------------------------------------------------------------------|
| **Data Collection**  | IoT Sensors + MySQL     | Continuous data from sensors stored in a central MySQL database.   |
| **ML Backend**       | Scikit-learn, XGBoost   | Forecasting, anomaly detection, optimization models, NLP based recommendation module           |
| **API Layer**        | FastAPI (RESTful)       | Connects backend, ML models, and frontend for seamless data flow.  |
| **Frontend**         | Next.js                 | Real-time visualizations and user interactions.                    |

## 🌐 Deployment Overview

- **Backend:** FastAPI, hosted on Render
- **Database:** MySQL on Railway
- **Frontend:** Next.js, deployed on Vercel

**Security:** Backend connects securely to the cloud database using environment variables. The frontend communicates with the backend via a public API.

## 🛠️ Local Development Guide

### 1. Set Up the MySQL Database

- Install MySQL.
- Create a new database (e.g., `SmartHomeEnergyUsageDB`).
- Import the provided `.sql` file to create tables and insert sample data.

### 2. Backend Setup

```bash
cd fastapi-backend
# Create a .env file in the backend directory with your database credentials:
# DB_HOST=localhost
# DB_USER=root
# DB_PASSWORD=yourpassword
# DB_NAME=SmartHomeEnergyUsageDB

pip install -r requirements.txt
uvicorn main:app --reload
```

### 3. Frontend Setup

```bash
npm install
npm run dev
```

## 📊 Screenshots
<img width="432" alt="Screenshot 2025-06-15 at 3 03 10 PM" src="https://github.com/user-attachments/assets/7fb6430b-3a3c-4230-829f-8f243e719086" /><br>
<img width="432" alt="Screenshot 2025-06-15 at 3 02 53 PM" src="https://github.com/user-attachments/assets/0325d82a-1b16-4619-8db5-557f58934cf1" />




## 📄 License

This project is licensed under the MIT License.

**SYNERGY** — Make your home smarter, greener, and more efficient!
