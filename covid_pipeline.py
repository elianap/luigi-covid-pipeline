import os
import datetime

import luigi
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from luigi import LocalTarget, Task
from luigi.contrib.external_program import ExternalProgramTask
from luigi.parameter import IntParameter, Parameter, DateParameter, ListParameter
from matplotlib.dates import DateFormatter
from pathlib import Path

# TODO

output_dir = f"{os.getcwd()}/output"


class DownloadDataset(luigi.Task):

    dataset_version = DateParameter(default=datetime.date.today())
    dataset_name = Parameter(default="covidIT")

    columns_ita_eng = {
        "data": "date",
        "stato": "country",
        "ricoverati_con_sintomi": "hospitalized_with_symptoms",
        "terapia_intensiva": "intensive_care",
        "totale_ospedalizzati": "total_hospitalized",
        "isolamento_domiciliare": "home_confinement",
        "totale_positivi": "total_positive",
        "variazione_totale_positivi": "total_positive_change",
        "nuovi_positivi": "new_positives",
        "dimessi_guariti": "recovered",
        "deceduti": "death",
        "casi_da_sospetto_diagnostico": "positive_cases_from_clinical_activity",
        "casi_da_screening": "screening_cases",
        "totale_casi": "total_cases",
        "tamponi": "performed_tests",
        "casi_testati": "total_people_tested",
        "note": "notes",
        "ingressi_terapia_intensiva": "new_entries_intensive_care",
        "note_test": "notes_tests",
        "note_casi": "notes_cases",
    }
    data_url = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale.csv"
    output_folder = f"{output_dir}/dataset"

    def output(self):
        return LocalTarget(
            f"{self.output_folder}/{self.dataset_name}_v{self.dataset_version}.csv"
        )

    def run(self):
        df_data = self.load_data(self.data_url, columns_new_names=self.columns_ita_eng)

        Path(self.output_folder).mkdir(parents=True, exist_ok=True)
        df_data.to_csv(self.output().path)

    def load_data(self, data_url, columns_new_names=None):
        data = pd.read_csv(data_url)
        if columns_new_names:
            data.rename(columns=columns_new_names, inplace=True)
        data["date"] = pd.to_datetime(data["date"])
        data.set_index("date", inplace=True)
        return data


class DataPreProcessing(luigi.Task):

    dataset_version = DateParameter(default=datetime.date.today())
    dataset_name = Parameter(default="covidIT")

    def requires(self):
        return DownloadDataset(self.dataset_version, self.dataset_name)

    output_folder = f"{output_dir}/processed"

    def output(self):
        return LocalTarget(
            f"{self.output_folder}/{self.dataset_name}_processed_v{self.dataset_version}.csv"
        )

    def run(self):
        df_data = pd.read_csv(self.input().path, index_col="date")
        df_data = self.preprocess_data(df_data)

        Path(self.output_folder).mkdir(parents=True, exist_ok=True)
        df_data.to_csv(self.output().path)

    def preprocess_data(self, df_data):
        df_data["diff_death"] = df_data["death"].diff()
        df_data["diff_intensive_care"] = df_data["intensive_care"].diff()
        df_data["diff_performed_tests"] = df_data["performed_tests"].diff()
        return df_data


class PlotTrend(luigi.Task):

    dataset_version = DateParameter(default=datetime.date.today())
    dataset_name = Parameter(default="covidIT")
    attribute = Parameter(default="intensive_care")

    def requires(self):
        return DataPreProcessing(self.dataset_version, self.dataset_name)

    output_folder = f"{output_dir}/trend"

    def output(self):
        return LocalTarget(
            f"{self.output_folder}/{self.dataset_name}_trend_{self.attribute}_v{self.dataset_version}.png"
        )

    def run(self):
        df_data = pd.read_csv(self.input().path, index_col="date")
        fig = self.plotDateTrend(df_data.index, df_data[self.attribute], self.attribute)

        Path(self.output_folder).mkdir(parents=True, exist_ok=True)
        fig.savefig(self.output().path)

    def plotDateTrend(self, x_date, y, attribute, interval=20):
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.grid()
        ax.scatter(x_date, y, s=3)
        ax.set(xlabel="Date", ylabel=attribute, title=attribute)
        date_form = DateFormatter("%d-%m")
        ax.xaxis.set_major_formatter(date_form)
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=interval))
        return fig


class AggregateInReport(luigi.Task):

    dataset_version = DateParameter(default=datetime.date.today())
    dataset_name = Parameter(default="covidIT")
    output_folder = f"{output_dir}/report_trends"

    # --> Alternative for dynamic report --> run as --attributes '["intensive_care", "total_positive", "death"]'
    # attributes = ListParameter(default=["intensive_care", "total_positive", "recovered"])
    #
    attributes = ["intensive_care", "total_positive", "recovered"]

    def requires(self):
        return {
            attribute: PlotTrend(self.dataset_version, self.dataset_name, attribute)
            for attribute in self.attributes
        }

    def output(self):
        return LocalTarget(
            f"{self.output_folder}/{self.dataset_name}_report_trends_v{self.dataset_version}.html"
        )

    def run(self):
        path_by_attribute = {k: self.input()[k].path for k in self.input()}

        plots_html = self.getHTMLTrends(path_by_attribute)

        Path(self.output_folder).mkdir(parents=True, exist_ok=True)

        with open(self.output().path, "w") as fp:
            for plot_html in plots_html:
                fp.write(plot_html)

    def getHTMLTrends(self, path_by_attribute):
        plots_html = [
            f"<h2 style='text-align: center'>{k}</h2>\n<p style='text-align: center'><img src='{path_by_attribute[k]}'  style='width: 50%; height: 50%' /> </p>"
            for k in path_by_attribute
        ]
        return plots_html


class DataTransform(luigi.Task):

    dataset_version = DateParameter(default=datetime.date.today())
    dataset_name = Parameter(default="covidIT")
    attribute = Parameter(default="intensive_care")
    window_size = IntParameter(default=7)

    def requires(self):
        return DataPreProcessing(self.dataset_version, self.dataset_name)

    output_folder = f"{output_dir}/transformed_window"

    def output(self):
        return LocalTarget(
            f"{self.output_folder}/{self.dataset_name}_transformed_window_w{self.window_size}_{self.attribute}_v{self.dataset_version}.csv"
        )

    def run(self):
        df_data = pd.read_csv(self.input().path, index_col="date")
        df_windows = self.getXyWindow_df(
            df_data[self.attribute], window=self.window_size
        )
        df_windows = df_windows.dropna()

        Path(self.output_folder).mkdir(parents=True, exist_ok=True)
        df_windows.to_csv(self.output().path)

    def getXyWindow_df(self, d_attribute, window=7):
        attribute = d_attribute.name
        df_windows = pd.DataFrame(d_attribute)
        for i in range(1, window + 1):
            df_windows[f"v_t-{i}"] = df_windows[attribute].shift(i)
        df_windows[[attribute] + [f"v_t-{i}" for i in range(1, window + 1)]]
        return df_windows


class Modeling(luigi.Task):

    dataset_version = DateParameter(default=datetime.date.today())
    dataset_name = Parameter(default="covidIT")
    attribute = Parameter(default="intensive_care")
    window_size = IntParameter(default=7)
    model_name = Parameter(default="LR")

    def requires(self):
        return DataTransform(
            self.dataset_version, self.dataset_name, self.attribute, self.window_size
        )

    output_folder = f"{output_dir}/model"

    def output(self):
        return LocalTarget(
            f"{self.output_folder}/{self.dataset_name}_model_{self.attribute}_w{self.window_size}_{self.model_name}_v{self.dataset_version}.pkl"
        )

    def run(self):
        df_windows = pd.read_csv(self.input().path, index_col="date")
        df_windows.index = pd.to_datetime(df_windows.index)
        regr = self.modeling(
            df_windows,
            self.attribute,
            regressor=self.model_name,
        )

        Path(self.output_folder).mkdir(parents=True, exist_ok=True)

        import pickle

        with open(self.output().path, "wb") as f:
            pickle.dump(regr, f)

    def modeling(self, df_windows, attribute, regressor="LR", date_end_train=None):
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.linear_model import LinearRegression

        if regressor not in ["LR", "GBR"]:
            raise ValueError
        regr = LinearRegression() if regressor == "LR" else GradientBoostingRegressor()
        if date_end_train is None:
            date_end_train = df_windows.index[-1]

        X = df_windows.drop(columns=attribute)[:date_end_train].values
        y = df_windows[attribute][:date_end_train].values
        regr.fit(X, y)
        return regr


class PredictTrend(luigi.Task):

    dataset_version = DateParameter(default=datetime.date.today())
    dataset_name = Parameter(default="covidIT")
    attribute = Parameter(default="intensive_care")
    window_size = IntParameter(default=7)
    model_name = Parameter(default="LR")
    n_days_to_predict = IntParameter(default=7)

    def requires(self):
        return {
            "model": Modeling(
                self.dataset_version,
                self.dataset_name,
                self.attribute,
                self.window_size,
                self.model_name,
            ),
            "data_transformed": DataTransform(
                self.dataset_version,
                self.dataset_name,
                self.attribute,
                self.window_size,
            ),
        }

    output_folder = f"{output_dir}/prediction"

    def output(self):
        return LocalTarget(
            f"{self.output_folder}/{self.dataset_name}_prediction_{self.attribute}_w{self.window_size}_{self.model_name}_N{self.n_days_to_predict}_v{self.dataset_version}.csv"
        )

    def run(self):
        df_date = pd.read_csv(self.input()["data_transformed"].path, index_col="date")
        df_date.index = pd.to_datetime(df_date.index)

        import pickle

        with open(self.input()["model"].path, "rb") as f:
            regr = pickle.load(f)

        df_windows_pred = self.predictWindowing(
            df_date,
            self.attribute,
            regr,
            self.window_size,
            self.n_days_to_predict,
        )

        Path(self.output_folder).mkdir(parents=True, exist_ok=True)

        df_windows_pred.to_csv(self.output().path)

    def predictWindowing(
        self, df_windows_train, attribute, regr, window, n_days_to_predict=10
    ):

        date_end_train_date = df_windows_train.index[-1].date()
        date_previous_window = date_end_train_date - datetime.timedelta(days=window + 1)
        df_test_window = pd.DataFrame(
            df_windows_train[date_previous_window:date_end_train_date][attribute]
        )
        test_window = df_test_window[attribute].values
        start_i = len(test_window)
        X_test_prog = []
        y_pred_prog = []
        for i in range(start_i, start_i + n_days_to_predict):
            # X: |window| preceding samples
            X_test_i = test_window[i - window : i][::-1]
            X_test_prog.append(X_test_i)
            # y: regressor estimation given |window| preceding samples
            y_pred_prog.append(regr.predict([X_test_i])[0])
            test_window = np.append(test_window, regr.predict([X_test_i])[0])

        # Dataframe X |window| preceding samples
        df_pred = pd.DataFrame(
            X_test_prog, columns=[f"v_t-{i}" for i in range(1, window + 1)]
        )
        # y_predicted --> y estimated by the regressor
        df_pred[f"y_pred_{attribute}"] = y_pred_prog

        # Add date and sed as index
        start_pred_date = date_end_train_date + datetime.timedelta(days=1)
        datelist = pd.date_range(start_pred_date, periods=n_days_to_predict)
        df_pred.set_index(datelist, inplace=True)
        df_pred.index.name = "date"

        return df_pred


class PlotFutureTrend(luigi.Task):

    dataset_version = DateParameter(default=datetime.date.today())

    dataset_name = Parameter(default="covidIT")
    attribute = Parameter(default="intensive_care")
    window_size = IntParameter(default=7)
    model_name = Parameter(default="LR")
    n_days_to_predict = IntParameter(default=7)

    def requires(self):
        return {
            "data_pred": PredictTrend(
                self.dataset_version,
                self.dataset_name,
                self.attribute,
                self.window_size,
                self.model_name,
                self.n_days_to_predict,
            ),
            "data_transformed": DataTransform(
                self.dataset_version,
                self.dataset_name,
                self.attribute,
                self.window_size,
            ),
        }

    output_folder = f"{output_dir}/report_future_trend"

    def output(self):
        return LocalTarget(
            f"{self.output_folder}/{self.dataset_name}_future_trend_{self.attribute}_w{self.window_size}_N{self.n_days_to_predict}_{self.model_name}_v{self.dataset_version}.png"
        )

    def run(self):
        df_windows_pred = pd.read_csv(self.input()["data_pred"].path, index_col="date")
        df_windows_pred.index = pd.to_datetime(df_windows_pred.index)
        df_date = pd.read_csv(self.input()["data_transformed"].path, index_col="date")
        df_date.index = pd.to_datetime(pd.to_datetime(df_date.index).date)

        import datetime

        fig = self.plotEstimatedTrend(df_date, df_windows_pred, self.attribute)

        Path(self.output_folder).mkdir(parents=True, exist_ok=True)

        fig.savefig(self.output().path)

    def plotEstimatedTrend(
        self,
        df_date,
        df_windows_predicted,
        attribute,
        start_train=None,
        date_end_train=None,
        interval=15,
    ):
        import datetime

        # Starting date of the plot
        start_train = df_date.index[0] if start_train is None else start_train
        # End date of the true label/value of the plot
        date_end_train = df_date.index[-1] if date_end_train is None else date_end_train

        start_test = date_end_train.date() + datetime.timedelta(days=1)
        if df_windows_predicted[start_test:].empty:
            # TODO
            raise ValueError

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.grid()
        # Observed trend until training date
        x_date = df_date[start_train:date_end_train].index
        y_train = df_date[start_train:date_end_train][attribute].values
        ax.scatter(x_date, y_train, s=3, color="blue", label=attribute)
        # Predicted future trend
        ax.scatter(
            df_windows_predicted[start_test:].index,
            df_windows_predicted[start_test:][f"y_pred_{attribute}"].values,
            s=3,
            color="green",
            label=f"{attribute} predicted",
        )
        ax.legend()
        ax.set(xlabel="Date", ylabel=attribute, title=attribute)
        date_form = DateFormatter("%d-%m")
        ax.xaxis.set_major_formatter(date_form)
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=interval))
        return fig


if __name__ == "__main__":
    luigi.run()
