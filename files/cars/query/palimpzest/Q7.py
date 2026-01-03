import os
import pandas as pd
import palimpzest as pz
import copy

from palimpzest.core.lib.schemas import ImageFilepath, AudioFilepath


audio_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "audio_id", "type": int, "desc": "The integer id for the audio"},
    {"name": "audio_path", "type": AudioFilepath, "desc": "The filepath containing the audio"},
]

text_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "complaint_id", "type": int, "desc": "The integer id for the complaint"},
    {"name": "summary", "type": str, "desc": "The text summary of the complaint"},
]

image_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "image_id", "type": int, "desc": "The integer id for the image"},
    {"name": "image_path", "type": ImageFilepath, "desc": "The filepath containing the image"},
]


class AudioDataset(pz.IterDataset):
    def __init__(self, id: str, df: pd.DataFrame):
        super().__init__(id=id, schema=audio_cols)
        self.df = df
    def __len__(self):
        return len(self.df)
    def __getitem__(self, idx: int):
        return self.df.iloc[idx].to_dict()


class TextDataset(pz.IterDataset):
    def __init__(self, id: str, df: pd.DataFrame):
        super().__init__(id=id, schema=text_cols)
        self.df = df
    def __len__(self):
        return len(self.df)
    def __getitem__(self, idx: int):
        return self.df.iloc[idx].to_dict()


class ImageDataset(pz.IterDataset):
    def __init__(self, id: str, df: pd.DataFrame):
        super().__init__(id=id, schema=image_cols)
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
    complaints = pd.read_csv(os.path.join(data_dir, "data", f"text_complaints_data_{scale_factor}.csv"))

    # Pipeline 1: Audio - worn brakes
    audio_joined = cars.merge(audio, on='car_id', how='inner')[['car_id', 'audio_id', 'audio_path']]
    config_audio = copy.deepcopy(pz_config)
    ds_audio = AudioDataset(id="audio-data", df=audio_joined)
    ds_audio = ds_audio.sem_filter('You are given an audio recording of car diagnostics. Return true if the car from the recording has worn out brakes.', depends_on=['audio_path'])
    result_audio = ds_audio.run(config_audio)

    # Pipeline 2: Text - electrical problems
    text_joined = cars.merge(complaints, on='car_id', how='inner')[['car_id', 'complaint_id', 'summary']]
    config_text = copy.deepcopy(pz_config)
    ds_text = TextDataset(id="text-data", df=text_joined)
    ds_text = ds_text.sem_filter('In the complaint, the car has some problems with electrical system / connected to electrical system. Complaint: {summary}.', depends_on=['summary'])
    result_text = ds_text.run(config_text)

    # Pipeline 3: Image - dented
    image_joined = cars.merge(images, on='car_id', how='inner')[['car_id', 'image_id', 'image_path']]
    config_image = copy.deepcopy(pz_config)
    ds_image = ImageDataset(id="image-data", df=image_joined)
    ds_image = ds_image.sem_filter('You are given an image of a vehicle or its parts. Return true if car is dented.', depends_on=['image_path'])
    result_image = ds_image.run(config_image)

    # Combine results (UNION)
    df_audio = result_audio.to_df()[['car_id']]
    df_text = result_text.to_df()[['car_id']]
    df_image = result_image.to_df()[['car_id']]

    combined = pd.concat([df_audio, df_text, df_image]).drop_duplicates()

    return combined
