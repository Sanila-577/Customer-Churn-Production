import logging
import os
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
logging.basicConfig(level=logging.INFO, format=
    '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FeatureBinningStrategy(ABC):
    @abstractmethod
    def bin_feature(self, df: pd.DataFrame, column: str) ->pd.DataFrame:
        pass

class CustomBinningStratergy(FeatureBinningStrategy):
    def __init__(self, bin_definitions):
        self.bin_definitions = bin_definitions
        
        # storage for bin edges created by qcut (used during inference to map values to categories)
        self._charge_bin_edges = None
        logger.info(f"CustomBinningStrategy initialized with bins: {list(bin_definitions.keys())}") 

    def bin_charges(self, df, column):
        try:
            logger.info(f"Binning {column} using qcut into Low, Medium, High")
            print(df[column])
            # Attempt 3 quantile bins
            cat, bins = pd.qcut(
                df[column],
                q=3,
                labels=["Low", "Medium", "High"],
                retbins=True,
                duplicates='drop'
            )

            # If qcut drops duplicates, bins may be fewer than labels
            if len(bins) - 1 != 3:
                logger.warning(f"⚠ Only {len(bins) - 1} unique bins detected; reducing labels accordingly.")
                labels = ["Low", "High"] if len(bins) - 1 == 2 else ["Low"]
                cat = pd.qcut(df[column], q=len(bins) - 1, labels=labels, retbins=False, duplicates='drop')

            df['Charge_category'] = cat

            # Store edges
            labels = df['Charge_category'].cat.categories
            intervals = {}
            for i, label in enumerate(labels):
                left = float(bins[i])
                right = float(bins[i+1])
                intervals[label] = (left, right)
            self._charge_bin_edges = intervals

            logging.info(f"Charge bin edges set to: {self._charge_bin_edges}")
            logger.info("✓ MonthlyCharges binned successfully.")

            self.save_charge_bins("artifacts/charge_bins.npy")
            logger.info("✓ Charge bin edges saved to artifacts/charge_bins.npy")
            return self._charge_bin_edges

        except Exception as e:
            logger.warning(f"⚠ qcut failed for {column}: {e}")
            df['Charge_category'] = "Invalid"
            # Fallback: create a dummy single bin interval to prevent NoneType errors
            self._charge_bin_edges = {"All": (float(df[column].min()), float(df[column].max()))}
            return self._charge_bin_edges



    def bin_feature(self, df, column):
        logger.info(f"\n{'='*60}")
        logger.info(f"FEATURE BINNING - {column.upper()}")
        logger.info(f"{'='*60}")
        logger.info(f"Starting binning for column: {column}")

        # Handle tenure bins (manual binning)
        if column.lower() == "tenure":
            def assign_tenure_bin(value):
                for bin_label, bin_range in self.bin_definitions.items():
                    if len(bin_range) == 2 and bin_range[0] <= value <= bin_range[1]:
                        return bin_label
                return "Invalid"

            df['Tenture_category'] = df[column].apply(assign_tenure_bin)


        # Handle MonthlyCharges using qcut (quantile-based)
        elif column.lower() == "monthlycharges":
            self.bin_charges(df, column)

        # Log binning results
        if 'Tenture_category' in df.columns:
            bin_counts_tenure = df['Tenture_category'].value_counts()
            for bin_name, count in bin_counts_tenure.items():
                logger.info(f"  ✓ {bin_name}: {count} ({count/len(df)*100:.2f}%)")

        if 'Charge_category' in df.columns:
            bin_counts_charge = df['Charge_category'].value_counts()
            for bin_name, count in bin_counts_charge.items():
                logger.info(f"  ✓ {bin_name}: {count} ({count/len(df)*100:.2f}%)")

        # ✅ Return the modified DataFrame
        return df


    def service_count(self, df, columns):
        df["Service_count"] = (df[columns] == "Yes").sum(axis=1)
        logger.info("✓ Service count added")
        return df


    def bundle_user(self, df, columns):
        df["Bundle_user"] = np.where((df[columns[0]] != "No") & (df[columns[1]] == "Yes"), 1, 0)
        logger.info(f"✓ Bundle user flag added: {df['Bundle_user'].unique()}")
        logger.info("✓ Bundle user flag added")
        return df


    # ---------- Helpers for inference and mapping ----------

    def save_charge_bins(self, path: str):
        """Save the stored charge bin edges to a .npy file for later reuse during inference.

        Overwrites existing file at path.
        """
        if self._charge_bin_edges is None:
            raise RuntimeError("No charge bin edges to save. Run bin_charges() first.")

        dirpath = os.path.dirname(path)
        if dirpath:
            try:
                os.makedirs(dirpath, exist_ok=True)
            except Exception as e:
                logger.error(f"✗ Failed to create directory for saving bins: {e}")
                raise

        try:
            np.save(path, self._charge_bin_edges, allow_pickle=True)
            logger.info(f"✓ Charge bin edges saved to {path}")
        except Exception as e:
            logger.error(f"✗ Failed to save charge bin edges to {path}: {e}")
            raise


    def load_charge_bins(self, path: str):
        """Load charge bin edges from a .npy file and store them for inference mapping."""
        if not os.path.exists(path):
            logger.error(f"✗ Charge bin file not found at {path}")
            raise FileNotFoundError(f"Charge bin file not found at {path}")

        try:
            self._charge_bin_edges = np.load(path, allow_pickle=True).item()
            logger.info(f"✓ Charge bin edges loaded from {path}")
            return self._charge_bin_edges
        except Exception as e:
            logger.error(f"✗ Failed to load charge bin edges from {path}: {e}")
            raise

    def get_charge_bin_edges(self):
        """Return the stored charge bin intervals dict (label -> (left, right)).

        Returns None if MonthlyCharges haven't been binned yet.
        """
        return self._charge_bin_edges

    def get_charge_category_for_value(self, value):
        """Map a single MonthlyCharges value to its charge category using stored interval dict.

        Returns one of the labels (e.g., 'Low','Medium','High') or np.nan if value is NaN or mapping fails.
        """
        # handle NaN
        if pd.isna(value):
            return np.nan


        if not self._charge_bin_edges:
            # no intervals defined
           logging.error("No charge bin edges defined. Cannot map value to category.")
           raise RuntimeError("No charge bin edges defined. Cannot map value to category.")
        # check intervals
        for label, (left, right) in self._charge_bin_edges.items():
            # include left and right bounds
            if value >= left and value <= right:
                return label

        # fallback: choose nearest center
        try:
            labels = list(self._charge_bin_edges.keys())
            centers = [(self._charge_bin_edges[l][0] + self._charge_bin_edges[l][1]) / 2.0 for l in labels]
            dists = [abs(value - c) for c in centers]
            return labels[int(np.argmin(dists))]
        except Exception:
            return np.nan

    def assign_charge_category_series(self, series: pd.Series):
        pass

    def bin_monthly_charges(self, obj):
        """Convenience method for streaming or batch inference.

        Accepts either a pandas Series of MonthlyCharges or a DataFrame containing a 'MonthlyCharges' column.
        It will ensure self._charge_bin_edges exists (compute from obj if possible) and return a Series
        of categories or set df['Charge_category'] when passed a DataFrame.
        """
        # If passed a DataFrame, extract series
        if isinstance(obj, pd.DataFrame):
            if 'MonthlyCharges' not in obj.columns:
                raise ValueError("DataFrame must contain 'MonthlyCharges' column")
            series = obj['MonthlyCharges']
            created_df = True
        elif isinstance(obj, pd.Series):
            series = obj
            created_df = False
        else:
            raise TypeError('obj must be a pandas DataFrame or Series')
        
        # Ensure bin edges exist
        self._charge_bin_edges= self.load_charge_bins("artifacts/charge_bins.npy")
        
        print("hello")

        print("Binning charges to establish intervals...")
        # self._charge_bin_edges = self.bin_charges(obj,column="MonthlyCharges")
        print(self._charge_bin_edges)

        # Map values using stored intervals
        # intervals is self._charge_bin_edges, e.g. {"Low": (0.0, 29.85), "Medium": (29.85, 63.6), "High": (63.6, 150.0)}
        intervals = self._charge_bin_edges
        # sort by left edge
        sorted_items = sorted(intervals.items(), key=lambda kv: kv[1][0])
        labels = [lbl for lbl, _ in sorted_items]
        # bins must be length = n_labels + 1. Start with left of first interval, then use rights of intervals
        bins = [sorted_items[0][1][0]] + [r for _, (_, r) in sorted_items]

        # vectorized mapping
        mapped = pd.cut(series, bins=bins, labels=labels, include_lowest=True)
        print(mapped)
        if created_df:
            obj['Charge_category'] = mapped
            return obj
        return mapped

    def save_charge_bins(self, path: str):
        """Save the stored charge bin edges to a .npy file for later reuse during inference.

        Overwrites existing file at path.
        """
        if self._charge_bin_edges is None:
            raise RuntimeError("No charge bin edges to save. Run bin_charges() first.")

        dirpath = os.path.dirname(path)
        if dirpath:
            try:
                os.makedirs(dirpath, exist_ok=True)
            except Exception as e:
                logger.error(f"✗ Failed to create directory for saving bins: {e}")
                raise

        try:
            np.save(path, self._charge_bin_edges, allow_pickle=True)
            logger.info(f"✓ Charge bin edges saved to {path}")
        except Exception as e:
            logger.error(f"✗ Failed to save charge bin edges to {path}: {e}")
            raise

    def load_charge_bins(self, path: str):
        """Load charge bin edges from a .npy file and store them for inference mapping."""
        if not os.path.exists(path):
            logger.error(f"✗ Charge bin file not found at {path}")
            raise FileNotFoundError(f"Charge bin file not found at {path}")

        try:
            self._charge_bin_edges = np.load(path, allow_pickle=True).item()
            logger.info(f"✓ Charge bin edges loaded from {path}")
            return self._charge_bin_edges
        except Exception as e:
            logger.error(f"✗ Failed to load charge bin edges from {path}: {e}")
            raise
