import os
import pandas as pd
from lotus.types import ImageArray

def run(data_dir: str, scale_factor: int = 157376):
    # Load data
    cars = pd.read_csv(os.path.join(data_dir, "data", f"car_data_{scale_factor}.csv"))
    images = pd.read_csv(os.path.join(data_dir, "data", f"image_car_data_{scale_factor}.csv"))

    # Join cars with images
    joined = cars.merge(images, on='car_id', how='inner')

    # Filter for Manual transmission
    joined = joined[joined['transmission'] == 'Manual']

    # Apply semantic filter on images (car is NOT damaged)
    joined['image_path'] = joined['image_path'].apply(lambda x: ImageArray([x]))
    joined = joined.sem_filter('You are given an image of a vehicle or its parts. Return true if car is not damaged.', default=False)

    # Limit to 10
    return joined['car_id'].head(10)
