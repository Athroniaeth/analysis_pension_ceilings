import gradio as gr
import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from analysis_pension_ceilings import DATA_PATH
from analysis_pension_ceilings.process.pipeline.clean import pipeline_clean
from analysis_pension_ceilings.process.pipeline.postprocess import pipeline_postprocess
from analysis_pension_ceilings.process.pipeline.preprocess import pipeline_preprocess

path = DATA_PATH / "pension_ceilings.xlsx"

df = pl.read_excel(
    path,
    sheet_name="pension brute DD_carrière comp",
)


def plot_distribution(
    x: np.ndarray,
    y: np.ndarray,
    name: str = "Distribution Normale Discrète",
    x_label: str = "Prix",
    y_label: str = "Probabilité",
):
    """
    Plot the normal distribution.

    Args:
        x (np.ndarray): The x values of the distribution.
        y (np.ndarray): The y values of the distribution.
        name (str): The name of the plot.
        x_label (str): The x-axis label.
        y_label (str): The y-axis label.

    Returns:
        matplotlib.figure.Figure: The figure of the plot.
    """
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(x, y, width=0.5, alpha=0.7, edgecolor="k")
    ax.set_title(name)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    return fig


def pipeline_statistics_pension_ceilings(
    ceiling: int = 2000,
    average_open_ended: int = 8000,
    nbr_pensioners: int = 14_900_000,
):
    """
    Gradio pipeline, calculate the total benefits of the ceiling.

    Args:
        ceiling (int): The maximum value to cap the average pension values.
        average_open_ended (int): The value to use for the open-ended class.
        nbr_pensioners (int): The total number of pensioners in the population.

    Returns:
        dict: A dictionary with the total benefits calculated.
    """
    global df

    df = pipeline_clean(df)
    df = pipeline_preprocess(df, average_open_ended=average_open_ended)
    df = pipeline_postprocess(df, ceiling=ceiling, nbr_pensioners=nbr_pensioners)

    total_pension = df["total_average_pension"].sum()
    total_benefits = df["total_average_benefits"].sum()
    percentage_benefits = total_benefits / total_pension

    # Get number persons per pension average (uniq)
    x = df["average_pension"].to_numpy()
    y = df["number_pensioners"].to_numpy()
    before_plot = plot_distribution(x, y)

    # Get number persons per pension average after processing
    df_agg = df.group_by("average_pension_after_ceiling", maintain_order=True).agg(
        pl.col("number_pensioners").sum()
    )
    x = df_agg["average_pension_after_ceiling"].to_numpy()
    y = df_agg["number_pensioners"].to_numpy()
    after_process_plot = plot_distribution(x, y)

    return f"{percentage_benefits:.2%}", before_plot, after_process_plot


def _plot_distribution(mean: float, std: float, num_points: int):
    """
    Plot the normal distribution.

    Args:
        mean (float): The mean of the distribution.
        std (float): The standard deviation of the distribution.
        num_points (int): The number of points to plot.

    Returns:
        matplotlib.figure.Figure: The figure of the plot.
    """
    x = np.linspace(mean - 3 * std, mean + 3 * std, num_points)
    y = (1 / (std * np.sqrt(2 * np.pi))) * np.exp(-0.5 * ((x - mean) / std) ** 2)

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(x, y, width=0.5, alpha=0.7, edgecolor="k")
    ax.set_title("Distribution Normale Discrète")
    ax.set_xlabel("Prix")
    ax.set_ylabel("Probabilité")
    return fig


with gr.Blocks() as blocks:
    with gr.Row():
        with gr.Column(scale=2, variant="compact"):
            input_ceiling = gr.Number(label="Plafond des pensions", value=2000)
            input_average_open_ended = gr.Number(
                label="Moyenne ouverte des grandes pensions", value=8000, info="test"
            )
            input_nbr_pensioner = gr.Number(
                label="Nombre de pensionnaires", value=14_900_000
            )
            num_points_input = gr.Number(label="Nombre de Points", value=20, step=1)

            # Button to refresh the graph
            calcul_button = gr.Button("Calculer")

        with gr.Column(scale=5):
            output_plot = gr.Plot(label="Graphique de Distribution")
            output_plot2 = gr.Plot(label="Graphique de Distribution")

        with gr.Column(scale=1):
            output_percentage = gr.Textbox(label="Pourcentage des bénéfices")

    blocks.load(
        fn=pipeline_statistics_pension_ceilings,
        inputs=[input_ceiling, input_average_open_ended, input_nbr_pensioner],
        outputs=[output_percentage, output_plot, output_plot2],
    )

    calcul_button.click(
        fn=pipeline_statistics_pension_ceilings,
        inputs=[input_ceiling, input_average_open_ended, input_nbr_pensioner],
        outputs=[output_percentage, output_plot, output_plot2],
    )

    # Link between the inputs, and the graph
    """    calcul_button.click(
        fn=plot_distribution,
        inputs=[input_ceiling, input_nbr_pensioner, num_points_input],
        outputs=output_plot,
    )"""
