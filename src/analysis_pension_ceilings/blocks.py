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
    x_label: str = "Prix",
    y_label: str = "Probabilité",
):
    """
    Plot the normal distribution.

    Args:
        x (np.ndarray): The x values of the distribution.
        y (np.ndarray): The y values of the distribution.
        x_label (str): The x-axis label.
        y_label (str): The y-axis label.

    Returns:
        matplotlib.figure.Figure: The figure of the plot.
    """
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(x, y, width=0.5, alpha=0.7, edgecolor="orange")
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    return fig


def pipeline_statistics_pension_ceilings(
    ceiling: int = 2_000,
    average_open_ended: int = 8_000,
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

    df_result = pipeline_clean(df)
    df_result = pipeline_preprocess(df_result, average_open_ended=average_open_ended)
    df_result = pipeline_postprocess(df_result, ceiling=ceiling, nbr_pensioners=nbr_pensioners)

    total_pension = df_result["total_average_pension"].sum() * 12
    total_benefits = df_result["total_average_benefits"].sum() * 12
    percentage_benefits = total_benefits / total_pension

    # Get number persons per pension average (uniq)
    x = df_result["average_pension"].to_numpy()
    y = df_result["number_pensioners"].to_numpy()
    plot_before = plot_distribution(x, y)

    # Get number persons per pension average after processing
    df_agg = df_result.group_by("average_pension_after_ceiling", maintain_order=True).agg(
        pl.col("number_pensioners").sum()
    )
    x = df_agg["average_pension_after_ceiling"].to_numpy()
    y = df_agg["number_pensioners"].to_numpy()
    plot_after = plot_distribution(x, y)

    return (
        f"{total_pension:,.0f}",
        f"{total_benefits:,.0f}",
        f"{percentage_benefits:.2%}",
        plot_before,
        plot_after,
    )


with gr.Blocks(theme="default") as blocks:
    with gr.Row():
        with gr.Column(scale=1, variant="compact"):
            gr.Markdown("### Inputs")
            input_ceiling = gr.Number(
                label="Plafond des pensions",
                info="À partir de combien, on plafonne les pensions",
                value=2_000,
            )
            input_average_open_ended = gr.Number(
                label="Moyenne ouverte des grandes pensions",
                value=8000,
                info="Valeur à utiliser pour la classe ouverte",
            )
            input_nbr_pensioner = gr.Number(
                label="Nombre de pensionnaires",
                value=14_900_000,
                info="Nombre total de pensionnaires dans la population",
            )

        with gr.Column(scale=1, variant="compact"):
            gr.Markdown("### Outputs")
            output_total_pension = gr.Textbox(label="Total des pensions", info="Total des coûts par ans")
            output_total_benefits = gr.Textbox(label="Total des bénéfices", info="Total des bénéfices par ans")
            output_percentage = gr.Textbox(
                label="Pourcentage de bénéfices",
                info="Pourcentage des bénéfices par rapport aux pensions",
            )

            # Button to refresh the graph
            calcul_button = gr.Button("Calculer", variant="primary")

        with gr.Column(scale=5):
            gr.Markdown("### Graphiques")
            with gr.Tab("Before"):
                before_plot = gr.Plot(label="Graphique de Distribution")

            with gr.Tab("After"):
                after_plot = gr.Plot(label="Graphique de Distribution")

    blocks.load(
        fn=pipeline_statistics_pension_ceilings,
        inputs=[input_ceiling, input_average_open_ended, input_nbr_pensioner],
        outputs=[
            output_total_pension,
            output_total_benefits,
            output_percentage,
            before_plot,
            after_plot,
        ],
    )

    calcul_button.click(
        fn=pipeline_statistics_pension_ceilings,
        inputs=[input_ceiling, input_average_open_ended, input_nbr_pensioner],
        outputs=[
            output_total_pension,
            output_total_benefits,
            output_percentage,
            before_plot,
            after_plot,
        ],
    )
