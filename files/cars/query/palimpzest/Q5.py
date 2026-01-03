import os
import pandas as pd
import palimpzest as pz

from palimpzest.core.lib.schemas import ImageFilepath, AudioFilepath
from palimpzest.core.elements.groupbysig import GroupBySig


data_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "transmission", "type": str, "desc": "The transmission type of the car"},
    {"name": "image_id", "type": int, "desc": "The integer id for the image"},
    {"name": "image_path", "type": ImageFilepath, "desc": "The filepath containing the image"},
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
    images = pd.read_csv(os.path.join(data_dir, "data", f"image_car_data_{scale_factor}.csv"))
    audio = pd.read_csv(os.path.join(data_dir, "data", f"audio_car_data_{scale_factor}.csv"))

    # Join cars with images and audio
    joined = cars.merge(images, on='car_id', how='inner')
    joined = joined.merge(audio, on='car_id', how='inner')[['car_id', 'transmission', 'image_id', 'image_path', 'audio_id', 'audio_path']]

    # Create dataset
    dataset = MyDataset(id="cars-multimodal-data", df=joined)

    # Filter for Automatic transmission
    dataset = dataset.filter(lambda row: row['transmission'] == 'Automatic')

    # Apply semantic filters
    dataset = dataset.sem_filter('You are given an audio recording of car diagnostics. Return true if the recording captures an audio of a damaged car.', depends_on=['audio_path'])
    dataset = dataset.sem_filter('You are given an image of a vehicle or its parts. Return true if car is damaged.', depends_on=['image_path'])

    # Get distinct car_ids and count
    dataset = dataset.distinct(distinct_cols=['car_id'])

    # Execute
    output = dataset.run(pz_config)
    result_df = output.to_df()

    # Count
    count = len(result_df)

    return pd.DataFrame({'count': [count]})
