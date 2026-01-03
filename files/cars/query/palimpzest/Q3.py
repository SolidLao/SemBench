import os
import pandas as pd
import palimpzest as pz

from palimpzest.core.lib.schemas import ImageFilepath


data_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "transmission", "type": str, "desc": "The transmission type of the car"},
    {"name": "image_id", "type": int, "desc": "The integer id for the image"},
    {"name": "image_path", "type": ImageFilepath, "desc": "The filepath containing the image"},
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

    # Join cars with images
    joined = cars.merge(images, on='car_id', how='inner')[['car_id', 'transmission', 'image_id', 'image_path']]

    # Create dataset
    dataset = MyDataset(id="cars-images-data", df=joined)

    # Filter for Manual transmission
    dataset = dataset.filter(lambda row: row['transmission'] == 'Manual')

    # Apply semantic filter (car is NOT damaged)
    dataset = dataset.sem_filter('You are given an image of a vehicle or its parts. Return true if car is not damaged.', depends_on=['image_path'])

    # Limit to 10
    dataset = dataset.limit(10)

    # Execute and return
    output = dataset.run(pz_config)
    return output
