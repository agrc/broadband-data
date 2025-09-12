import pandas as pd

from broadband import utils


class TestConcatCategorical:
    def test_concat_dataframes_with_categoricals_gets_union_of_categories(self):
        df_a = pd.DataFrame(
            {
                "col1": pd.Categorical(["a", "b", "a"], categories=["a", "b"]),
            }
        )
        df_b = pd.DataFrame(
            {
                "col1": pd.Categorical(["b", "c", "c"], categories=["b", "c"]),
            }
        )

        result = utils.concat_dataframes_with_categoricals([df_a, df_b])
        assert result["col1"].dtype.categories.tolist() == ["a", "b", "c"]

    def test_concat_dataframes_with_categoricals_ignores_non_categorical_columns(self):
        df_a = pd.DataFrame(
            {
                "col1": pd.Categorical(["a", "b", "a"], categories=["a", "b"]),
                "col2": [1, 2, 3],
            }
        )
        df_b = pd.DataFrame(
            {
                "col1": pd.Categorical(["b", "c", "c"], categories=["b", "c"]),
                "col2": [4, 5, 6],
            }
        )

        result = utils.concat_dataframes_with_categoricals([df_a, df_b])
        assert result["col1"].dtype.categories.tolist() == ["a", "b", "c"]
        assert result["col2"].dtype == "int64"

    def test_concat_dataframes_ignores_index(self):
        df_a = pd.DataFrame(
            {
                "col1": pd.Categorical(["a", "b", "a"], categories=["a", "b"]),
                "col2": [1, 2, 3],
            },
            index=[10, 11, 12],
        )
        df_b = pd.DataFrame(
            {
                "col1": pd.Categorical(["b", "c", "c"], categories=["b", "c"]),
                "col2": [4, 5, 6],
            },
            index=[20, 21, 22],
        )

        result = utils.concat_dataframes_with_categoricals([df_a, df_b], ignore_index=True)
        assert result["col1"].dtype.categories.tolist() == ["a", "b", "c"]
        assert result.index.tolist() == [0, 1, 2, 3, 4, 5]
