import pandas as pd
import numpy as np
import joblib
from keras.models import Model
from keras.layers import LSTM, Dense, Dropout, Bidirectional, BatchNormalization, Input
from keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau
from keras.optimizers import Adam
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt

def create_unified_lstm_model(time_step, num_devices):
    """Create LSTM model with power and device inputs."""
    power_input = Input(shape=(time_step, 1), name='power_input')
    device_input = Input(shape=(1,), name='device_input')
    
    device_embedding = Dense(8)(device_input)
    device_embedding = Dense(4, activation='relu')(device_embedding)
    
    lstm = Bidirectional(LSTM(units=64, return_sequences=True, activation='tanh'))(power_input)
    lstm = BatchNormalization()(lstm)
    lstm = Dropout(0.3)(lstm)
    lstm = Bidirectional(LSTM(units=128, return_sequences=True, activation='tanh'))(lstm)
    lstm = BatchNormalization()(lstm)
    lstm = Dropout(0.4)(lstm)
    lstm = LSTM(units=64, return_sequences=False, activation='relu')(lstm)
    lstm = BatchNormalization()(lstm)
    lstm = Dropout(0.3)(lstm)
    
    lstm_flattened = Dense(32)(lstm)
    merged = tf.keras.layers.concatenate([lstm_flattened, device_embedding])
    dense = Dense(32, activation='relu')(merged)
    dense = BatchNormalization()(dense)
    dense = Dropout(0.2)(dense)
    output = Dense(1)(dense)
    
    model = Model(inputs=[power_input, device_input], outputs=output)
    optimizer = Adam(learning_rate=0.001)
    model.compile(optimizer=optimizer, loss='mean_squared_error')
    return model

def split_data(data, train_pct=0.8, val_pct=0.2):
    """Split data into train, validation, and test sets."""
    training_size = int(len(data) * train_pct)
    val_size = int(training_size * val_pct)
    train = data.iloc[:training_size-val_size]
    val = data.iloc[training_size-val_size:training_size]
    test = data.iloc[training_size:]
    return train, val, test

def create_dataset(dataset, time_step=1):
    """Create time series dataset with features and targets."""
    dataX, dataY = [], []
    power_values = dataset['Power_scaled'].values
    device_values = dataset['device_id'].values
    
    if len(power_values) <= time_step + 1:
        return np.array(dataX, dtype=object), np.array(dataY)
        
    for i in range(len(power_values) - time_step - 1):
        power_window = power_values[i:(i + time_step)]
        device_id_value = device_values[i + time_step - 1]
        features = [power_window, device_id_value]
        dataX.append(features)
        dataY.append(power_values[i + time_step])
    
    return np.array(dataX, dtype=object), np.array(dataY)

# Load and preprocess data
df = pd.read_csv('energy_readings.csv')
df.rename(columns={"timestamp": "DateTime", "power": "Power"}, inplace=True)

print(f"Dataset contains {len(df)} rows")
if len(df) > 50000:
    df = df.iloc[:50000]

# Filter to device_id = 1
unique_devices = df['device_id'].unique()
if len(unique_devices) != 1 or unique_devices[0] != 1:
    df = df[df['device_id'] == 1]

df["DateTime"] = pd.to_datetime(df["DateTime"])
df = df.set_index("DateTime")

device_id = 1
device_ids = np.array([device_id])

# Resample data to ensure sufficient data points
try:
    device_data = df.resample("h").mean()
    if len(device_data) < 200:
        device_data = df.resample("15T").mean()
    if len(device_data) < 200:
        device_data = df.resample("5T").mean()
    if len(device_data) < 200:
        device_data = df.copy()
except Exception:
    device_data = df.copy()

device_data['device_id'] = device_id
combined_data = device_data

if len(combined_data) < 150:
    raise ValueError(f"Dataset too small: {len(combined_data)} rows")

# Scale power values
power_scaler = MinMaxScaler(feature_range=(0, 1))
combined_data['Power_scaled'] = power_scaler.fit_transform(combined_data['Power'].values.reshape(-1, 1))
joblib.dump(power_scaler, 'power_scaler.joblib')

# Split data
train_data, val_data, test_data = split_data(combined_data)

# Create time series datasets
time_step = 100
if len(combined_data) < 500:
    time_step = min(50, len(combined_data) // 4)
    
X_train, y_train = create_dataset(train_data, time_step)
X_val, y_val = create_dataset(val_data, time_step)

if len(X_train) == 0:
    raise ValueError("No training data created")

# Prepare training data
X_train_power = np.stack([x[0] for x in X_train]).reshape(-1, time_step, 1)
X_train_device = np.array([x[1] for x in X_train]).reshape(-1, 1)

# Prepare validation data
if len(X_val) > 0:
    X_val_power = np.stack([x[0] for x in X_val]).reshape(-1, time_step, 1)
    X_val_device = np.array([x[1] for x in X_val]).reshape(-1, 1)
    validation_data = ([X_val_power, X_val_device], y_val)
else:
    validation_data = None
    validation_split = 0.2

# Train model
num_devices = len(device_ids)
model = create_unified_lstm_model(time_step, num_devices)

callbacks = [
    EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True),
    ReduceLROnPlateau(monitor='val_loss', factor=0.2, patience=5, min_lr=0.0001),
    ModelCheckpoint(filepath='unified_best_model.h5', monitor='val_loss', save_best_only=True)
]

if validation_data is not None:
    history = model.fit(
        [X_train_power, X_train_device], y_train,
        validation_data=validation_data,
        epochs=100, batch_size=32, callbacks=callbacks
    )
else:
    history = model.fit(
        [X_train_power, X_train_device], y_train,
        validation_split=validation_split,
        epochs=100, batch_size=32, callbacks=callbacks
    )

model.save('unified_lstm_model.h5')

# Evaluate model
X_test, y_test = create_dataset(test_data, time_step)
X_test_power = np.stack([x[0] for x in X_test]).reshape(-1, time_step, 1)
X_test_device = np.array([x[1] for x in X_test]).reshape(-1, 1)

test_loss = model.evaluate([X_test_power, X_test_device], y_test, verbose=0)
y_pred = model.predict([X_test_power, X_test_device])

# Calculate metrics
mse = np.mean((y_test - y_pred.flatten())**2)
mae = np.mean(np.abs(y_test - y_pred.flatten()))
rmse = np.sqrt(mse)

# Convert to actual power values
y_test_inv = power_scaler.inverse_transform(y_test.reshape(-1, 1)).flatten()
y_pred_inv = power_scaler.inverse_transform(y_pred).flatten()

mse_inv = np.mean((y_test_inv - y_pred_inv)**2)
mae_inv = np.mean(np.abs(y_test_inv - y_pred_inv))
rmse_inv = np.sqrt(mse_inv)

print(f"Test Loss (MSE): {test_loss:.6f}")
print(f"Normalized Metrics - MSE: {mse:.6f}, MAE: {mae:.6f}, RMSE: {rmse:.6f}")
print(f"Actual Power Metrics - MSE: {mse_inv:.6f}, MAE: {mae_inv:.6f}, RMSE: {rmse_inv:.6f}")

# Plot predictions vs actual
plt.figure(figsize=(12, 6))
max_samples = min(100, len(y_test_inv))
plt.plot(y_test_inv[:max_samples], label='Actual')
plt.plot(y_pred_inv[:max_samples], label='Predicted')
plt.title(f'Device {device_id} - Predictions vs Actual')
plt.xlabel('Sample Index')
plt.ylabel('Power')
plt.legend()
plt.grid(True)
plt.savefig(f'D:\\HPE\\LSTM\\device_{device_id}_test_accuracy.png')
plt.close()

# Performance summary
avg_power = np.mean(np.abs(y_test_inv))
mae_percentage = mae_inv / avg_power * 100
accuracy_percentage = 100 - mae_percentage

print(f"Performance Summary:")
print(f"Average power: {avg_power:.2f}")
print(f"Mean absolute error: {mae_inv:.2f}")
print(f"Error percentage: {mae_percentage:.2f}%")
print(f"Accuracy: {accuracy_percentage:.2f}%")