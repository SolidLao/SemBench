import os
import pandas as pd
import palimpzest as pz
import copy

from palimpzest.core.lib.schemas import ImageFilepath, AudioFilepath


# Define schemas for different joins
audio_text_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "audio_id", "type": int, "desc": "The integer id for the audio"},
    {"name": "audio_path", "type": AudioFilepath, "desc": "The filepath containing the audio"},
    {"name": "complaint_id", "type": int, "desc": "The integer id for the complaint"},
    {"name": "summary", "type": str, "desc": "The text summary of the complaint"},
]

image_text_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "image_id", "type": int, "desc": "The integer id for the image"},
    {"name": "image_path", "type": ImageFilepath, "desc": "The filepath containing the image"},
    {"name": "complaint_id", "type": int, "desc": "The integer id for the complaint"},
    {"name": "summary", "type": str, "desc": "The text summary of the complaint"},
]

image_audio_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "image_id", "type": int, "desc": "The integer id for the image"},
    {"name": "image_path", "type": ImageFilepath, "desc": "The filepath containing the image"},
    {"name": "audio_id", "type": int, "desc": "The integer id for the audio"},
    {"name": "audio_path", "type": AudioFilepath, "desc": "The filepath containing the audio"},
]


class AudioTextDataset(pz.IterDataset):
    def __init__(self, id: str, df: pd.DataFrame):
        super().__init__(id=id, schema=audio_text_cols)
        self.df = df
    def __len__(self):
        return len(self.df)
    def __getitem__(self, idx: int):
        return self.df.iloc[idx].to_dict()


class ImageTextDataset(pz.IterDataset):
    def __init__(self, id: str, df: pd.DataFrame):
        super().__init__(id=id, schema=image_text_cols)
        self.df = df
    def __len__(self):
        return len(self.df)
    def __getitem__(self, idx: int):
        return self.df.iloc[idx].to_dict()


class ImageAudioDataset(pz.IterDataset):
    def __init__(self, id: str, df: pd.DataFrame):
        super().__init__(id=id, schema=image_audio_cols)
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

    # Join all tables
    all_joined = cars.merge(images, on='car_id', how='left')
    all_joined = all_joined.merge(audio, on='car_id', how='left')
    all_joined = all_joined.merge(complaints, on='car_id', how='left')

    # Process each modality check in pandas first to identify damaged cars
    audio_damaged_ids = set()
    image_damaged_ids = set()
    text_damaged_ids = set()

    # Check audio damage
    audio_data = all_joined[all_joined['audio_id'].notna()][['car_id', 'audio_id', 'audio_path']].drop_duplicates()
    if len(audio_data) > 0:
        config_audio = copy.deepcopy(pz_config)
        ds_audio = pz.IterDataset(id="audio", schema=[{"name": "car_id", "type": int}, {"name": "audio_id", "type": int}, {"name": "audio_path", "type": AudioFilepath}])
        ds_audio.df = audio_data
        ds_audio = ds_audio.sem_filter('You are given an audio recording of car diagnostics. Return true if the recording captures an audio of a damaged car.', depends_on=['audio_path'])
        result_audio = ds_audio.run(config_audio)
        audio_damaged_ids = set(result_audio.to_df()['car_id'])

    # Check image damage
    image_data = all_joined[all_joined['image_id'].notna()][['car_id', 'image_id', 'image_path']].drop_duplicates()
    if len(image_data) > 0:
        config_image = copy.deepcopy(pz_config)
        ds_image = pz.IterDataset(id="image", schema=[{"name": "car_id", "type": int}, {"name": "image_id", "type": int}, {"name": "image_path", "type": ImageFilepath}])
        ds_image.df = image_data
        ds_image = ds_image.sem_filter('You are given an image of a vehicle or its parts. Return true if car is damaged.', depends_on=['image_path'])
        result_image = ds_image.run(config_image)
        image_damaged_ids = set(result_image.to_df()['car_id'])

    # Check text damage (fire/burned)
    text_data = all_joined[all_joined['complaint_id'].notna()][['car_id', 'complaint_id', 'summary']].drop_duplicates()
    if len(text_data) > 0:
        config_text = copy.deepcopy(pz_config)
        ds_text = pz.IterDataset(id="text", schema=[{"name": "car_id", "type": int}, {"name": "complaint_id", "type": int}, {"name": "summary", "type": str}])
        ds_text.df = text_data
        ds_text = ds_text.sem_filter('You are be given a textual complaint entailing that the car was in on fire or burned. Complaint: {summary}.', depends_on=['summary'])
        result_text = ds_text.run(config_text)
        text_damaged_ids = set(result_text.to_df()['car_id'])

    # XOR logic: at least one positive AND at least one negative
    result_ids = []
    for car_id in all_joined['car_id'].unique():
        car_data = all_joined[all_joined['car_id'] == car_id].iloc[0]

        has_audio = pd.notna(car_data['audio_id'])
        has_image = pd.notna(car_data['image_id'])
        has_text = pd.notna(car_data['complaint_id'])

        is_audio_damaged = car_id in audio_damaged_ids if has_audio else None
        is_image_damaged = car_id in image_damaged_ids if has_image else None
        is_text_damaged = car_id in text_damaged_ids if has_text else None

        statuses = [s for s in [is_audio_damaged, is_image_damaged, is_text_damaged] if s is not None]

        if len(statuses) >= 2:
            has_positive = any(statuses)
            has_negative = not all(statuses)

            if has_positive and has_negative:
                result_ids.append(car_id)

    return pd.DataFrame({'car_id': result_ids})
