import os
import pandas as pd
from lotus.types import ImageArray

def run(data_dir: str, scale_factor: int = 157376):
    # Load data
    cars = pd.read_csv(os.path.join(data_dir, "data", f"car_data_{scale_factor}.csv"))
    images = pd.read_csv(os.path.join(data_dir, "data", f"image_car_data_{scale_factor}.csv"))

    # Join cars with images
    joined = cars.merge(images, on='car_id', how='inner')

    # Apply semantic filter on images
    joined['image_path'] = joined['image_path'].apply(lambda x: ImageArray([x]))
    joined = joined.sem_filter('You are given an image of a vehicle or its parts. Return true if car has both, puncture and paint scratches.', default=False)

    # Limit to 100
    return joined['car_id'].head(100)
