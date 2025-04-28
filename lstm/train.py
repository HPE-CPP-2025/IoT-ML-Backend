import pandas as pd
import numpy as np
import joblib
from keras.models import Model
from keras.layers import LSTM, Dense, Dropout, Bidirectional, BatchNormalization, Input
from keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau
from keras.optimizers import Adam
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import os      # Keep os for path joining
import sys     # Keep sys for exit

# Define function to create a unified LSTM model
def create_unified_lstm_model(time_step, num_devices):
    power_input = Input(shape=(time_step, 1), name='power_input')
    device_input = Input(shape=(1,), name='device_input')
    # Consider simplifying or adjusting embedding if device IDs are just integers
    device_embedding = Dense(8)(device_input) # Simple embedding
    device_embedding = Dense(4, activation='relu')(device_embedding)

    lstm = Bidirectional(LSTM(units=64, return_sequences=True, activation='tanh'))(power_input)
    lstm = BatchNormalization()(lstm)
    lstm = Dropout(0.3)(lstm)

    lstm = Bidirectional(LSTM(units=128, return_sequences=True, activation='tanh'))(lstm)
    lstm = BatchNormalization()(lstm)
    lstm = Dropout(0.4)(lstm)

    # Change activation of the last LSTM layer from 'relu' to 'tanh' for potentially better stability
    lstm = LSTM(units=64, return_sequences=False, activation='tanh')(lstm) # Keep tanh activation
    lstm = BatchNormalization()(lstm)
    lstm = Dropout(0.3)(lstm)

    # Combine LSTM output and device embedding
    lstm_flattened = Dense(32)(lstm) # Optional dense layer after LSTM
    merged = tf.keras.layers.concatenate([lstm_flattened, device_embedding])

    dense = Dense(32, activation='relu')(merged)
    dense = BatchNormalization()(dense)
    dense = Dropout(0.2)(dense)

    output = Dense(1)(dense) # Final linear layer for regression

    model = Model(inputs=[power_input, device_input], outputs=output)
    optimizer = Adam(learning_rate=0.001)
    model.compile(optimizer=optimizer, loss='mean_squared_error')
    return model

# Load and preprocess data from CSV
df = None
# Look for CSV in the same directory as the script
csv_path = os.path.join(os.path.dirname(__file__), 'energy_readings.csv') 

try:
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path) # Load from CSV
    print(f"Data loaded ({len(df)} rows). Processing...")

    if df.empty:
        print(f"No data found in '{csv_path}'. Exiting.", file=sys.stderr)
        sys.exit(1) # Use sys.exit

except FileNotFoundError:
    print(f"Error: CSV file not found at {csv_path}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"\nAn unexpected error occurred during CSV loading: {e}", file=sys.stderr)
    sys.exit(1)

# Rename columns for consistency if needed later
# Ensure column names match the CSV ('timestamp', 'power', 'device_id')
df.rename(columns={"timestamp": "DateTime", "power": "Power"}, inplace=True)

# Preprocessing steps, scaling, and dataset splitting
df["DateTime"] = pd.to_datetime(df["DateTime"]) # Ensure DateTime type
# df = df.set_index("DateTime") # Set DateTime as index ONLY if resampling

device_ids = df['device_id'].unique()
# combined_data = pd.DataFrame() # Not needed if not resampling

# print("Resampling data to hourly means...")
# # --- Start: Comment out or remove resampling section ---
# # for device_id in device_ids:
# #     # Select data for the device and resample the 'Power' column
# #     # Use 'h' instead of deprecated 'H' if pandas version >= 2.2
# #     try:
# #         device_data = df[df['device_id'] == device_id][['Power']].resample("h").mean()
# #     except ValueError: # Handle older pandas versions that might not recognize 'h'
# #         print("Using deprecated 'H' for resampling due to pandas version or error with 'h'.", file=sys.stderr)
# #         device_data = df[df['device_id'] == device_id][['Power']].resample("H").mean()
# #     # Drop hours where the mean is NaN (i.e., no data in that hour)
# #     device_data.dropna(inplace=True)
# #     if not device_data.empty:
# #         device_data['device_id'] = device_id # Add device_id back
# #         combined_data = pd.concat([combined_data, device_data])
# #
# # if combined_data.empty:
# #     print("No data remaining after resampling. Check input data and resampling frequency.", file=sys.stderr)
# #     sys.exit(1)
# #
# # print(f"Resampling complete. Combined data has {len(combined_data)} rows.")
# # # Reset index so DateTime becomes a column again, useful for splitting/grouping if needed
# # combined_data.reset_index(inplace=True)
# # --- End: Comment out or remove resampling section ---

# Use the original DataFrame 'df' instead of 'combined_data'
combined_data = df # Assign df to combined_data to minimize changes below
combined_data['DateTime'] = pd.to_datetime(combined_data['DateTime']) # Ensure DateTime is correct type

# Scale power values
print("Scaling power values...")
power_scaler = MinMaxScaler(feature_range=(0, 1))
# Ensure 'Power' column exists and fit scaler
if 'Power' not in combined_data.columns or combined_data['Power'].isnull().all():
     # Adjusted error message as resampling is removed
     print("Error: 'Power' column is missing or all null in the loaded CSV data.", file=sys.stderr)
     sys.exit(1)
# Fit and transform the 'Power' column. Use [['Power']] to keep it as a DataFrame.
combined_data['Power_scaled'] = power_scaler.fit_transform(combined_data[['Power']])
scaler_filename = 'power_scaler.joblib'
joblib.dump(power_scaler, scaler_filename)
print(f"Power scaler saved to {scaler_filename}")

# Split data into train, validation, and test (e.g., 70/15/15 split)
# Ensure data is sorted by time before splitting
combined_data = combined_data.sort_values(by=['device_id', 'DateTime'])

def split_data(data, train_pct=0.7, val_pct=0.15):
    # Using global time split. Ensure data is sorted by DateTime first.
    data = data.sort_values('DateTime') # Sort globally by time before splitting
    n = len(data)
    train_end = int(n * train_pct)
    val_end = train_end + int(n * val_pct)
    train = data.iloc[:train_end]
    val = data.iloc[train_end:val_end]
    test = data.iloc[val_end:]
    print(f"Data split (globally by time): Train={len(train)}, Validation={len(val)}, Test={len(test)}")
    # Check if splits are empty
    if train.empty or val.empty:
         print("Warning: Training or Validation set is empty after split.", file=sys.stderr)
    return train, val, test

train_data, val_data, test_data = split_data(combined_data)

# Create time-series datasets suitable for LSTM
def create_dataset(dataset, time_step=1):
    dataX, dataY = [], []
    # Group by device to create sequences per device without mixing them
    # Ensure DateTime is sorted within each group before creating sequences
    grouped = dataset.sort_values('DateTime').groupby('device_id')
    for device_id, device_df in grouped:
        power_values = device_df['Power_scaled'].values
        # device_id is constant for this group
        if len(power_values) > time_step: # Need at least time_step + 1 points
            for i in range(len(power_values) - time_step): # Iterate up to the point where a full sequence + target is available
                power_window = power_values[i:(i + time_step)]
                # Append [power_window, device_id] as features
                dataX.append([power_window, device_id])
                # Append the next power value as the target
                dataY.append(power_values[i + time_step])
    if not dataX:
        print("Warning: create_dataset resulted in empty dataX. Check input dataset size and time_step.", file=sys.stderr)
        return np.array([]), np.array([])
    # Convert lists to numpy arrays, handling the object type for the mixed list
    return np.array(dataX, dtype=object), np.array(dataY).astype('float32') # Ensure Y is float

time_step = 100
print(f"Creating datasets with time_step={time_step}...")
X_train, y_train = create_dataset(train_data, time_step)
X_val, y_val = create_dataset(val_data, time_step)
X_test, y_test = create_dataset(test_data, time_step) # Create test set for evaluation

# Check if datasets are empty after creation
if X_train.size == 0 or X_val.size == 0:
    print("Error: Training or validation dataset is empty after sequence creation. Check data splitting and time_step.", file=sys.stderr)
    sys.exit(1)

print("Separating features for model input...")
# Separate power sequences and device IDs, ensuring correct types and shapes
X_train_power = np.array([x[0] for x in X_train]).astype('float32').reshape(-1, time_step, 1)
X_train_device = np.array([x[1] for x in X_train]).astype('float32').reshape(-1, 1) # Ensure device ID is float for the model layer
X_val_power = np.array([x[0] for x in X_val]).astype('float32').reshape(-1, time_step, 1)
X_val_device = np.array([x[1] for x in X_val]).astype('float32').reshape(-1, 1)
if X_test.size > 0:
    X_test_power = np.array([x[0] for x in X_test]).astype('float32').reshape(-1, time_step, 1)
    X_test_device = np.array([x[1] for x in X_test]).astype('float32').reshape(-1, 1)
else:
    X_test_power, X_test_device, y_test = None, None, None # Handle empty test set

# Train the model
num_devices = len(device_ids) # Total number of unique devices in original data
print(f"Number of unique device IDs in original data: {num_devices}")
print("Creating unified LSTM model...")
model = create_unified_lstm_model(time_step, num_devices)
model.summary() # Print model summary

# Define callbacks
callbacks = [
    EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True, verbose=1),
    ReduceLROnPlateau(monitor='val_loss', factor=0.2, patience=5, min_lr=0.0001, verbose=1),
    ModelCheckpoint(filepath='unified_best_model.h5', monitor='val_loss', save_best_only=True, verbose=1)
]

print("Starting model training...")
history = model.fit(
    [X_train_power, X_train_device], y_train,
    validation_data=([X_val_power, X_val_device], y_val),
    epochs=100, batch_size=32, callbacks=callbacks, verbose=1
)

# Save the final model (potentially the best one restored by EarlyStopping)
final_model_path = 'unified_lstm_model.h5'
model.save(final_model_path)
print(f"Training complete. Final model saved to {final_model_path}")

# Optional: Evaluate on test set if it exists
if X_test_power is not None and y_test is not None:
    print("Evaluating on test set...")
    test_loss = model.evaluate([X_test_power, X_test_device], y_test, verbose=0)
    print(f"Test Loss: {test_loss}")
else:
    print("Test set was empty or not created, skipping evaluation.")

print("Script finished.")