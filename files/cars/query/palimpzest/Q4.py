import os
import pandas as pd
import palimpzest as pz


data_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "year", "type": int, "desc": "The year of the car"},
    {"name": "complaint_id", "type": int, "desc": "The integer id for the complaint"},
    {"name": "summary", "type": str, "desc": "The text summary of the complaint"},
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
    complaints = pd.read_csv(os.path.join(data_dir, "data", f"text_complaints_data_{scale_factor}.csv"))

    # Join cars with complaints
    joined = cars.merge(complaints, on='car_id', how='inner')[['car_id', 'year', 'complaint_id', 'summary']]

    # Create dataset
    dataset = MyDataset(id="cars-complaints-data", df=joined)

    # Apply semantic filter
    dataset = dataset.sem_filter('In the complaint, the car has some problems with engine / connected to engine. Complaint: {summary}.', depends_on=['summary'])

    # Execute
    output = dataset.run(pz_config)
    result_df = output.to_df()

    # Calculate average age
    average_age = 2026 - result_df['year'].mean()

    return pd.DataFrame({'average_age': [average_age]})
