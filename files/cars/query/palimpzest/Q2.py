import os
import pandas as pd
import palimpzest as pz

from palimpzest.core.lib.schemas import AudioFilepath


data_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "fuel_type", "type": str, "desc": "The fuel type of the car"},
    {"name": "audio_id", "type": int, "desc": "The integer id for the audio"},
    {"name": "audio_path", "type": AudioFilepath, "desc": "The filepath containing the audio"},
]


class MyDataset(pz.IterDataset):
    def __init__(self, id: str, df: pd.DataFrame):
        super().__init__(id=id, schema=data_cols)
        self.df = df

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx: int):
        return self.df.iloc[idx].to_dict()


def run(pz_config, data_dir: str, scale_factor: int = 157376):
    # Load data
    cars = pd.read_csv(os.path.join(data_dir, "data", f"car_data_{scale_factor}.csv"))
    audio = pd.read_csv(os.path.join(data_dir, "data", f"audio_car_data_{scale_factor}.csv"))

    # Join cars with audio
    joined = cars.merge(audio, on='car_id', how='inner')[['car_id', 'fuel_type', 'audio_id', 'audio_path']]

    # Create dataset
    dataset = MyDataset(id="cars-audio-data", df=joined)

    # Filter for Electric cars
    dataset = dataset.filter(lambda row: row['fuel_type'] == 'Electric')

    # Apply semantic filter on audio
    dataset = dataset.sem_filter('You are given an audio recording of car diagnostics. Return true if the car from the recording has a dead battery, false otherwise.', depends_on=['audio_path'])

    # Execute and return
    output = dataset.run(pz_config)
    return output
