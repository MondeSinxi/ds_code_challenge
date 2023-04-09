import logging
import typer
from src.s3_select_hex import GeoQuery
from src.join_hex_index import join_geodata, run_checks, test_thresholds
from src.wind_geo_data import get_winds_data, join_wind_to_service, anonymise

app = typer.Typer()


@app.command()
def challenge_one(
    validate: bool = typer.Argument(
        True,
        help="Checks queried city-hex-polygons-8-10.geojson against reference city-hex-polygons-8.geojson",
    )
):
    gq = GeoQuery()
    if validate:
        if gq.is_valid:
            logging.info("H3 Resolution data has been read and is valid")
        else:
            logging.error("H3 Resolution data has been read and is NOT valid!")


@app.command()
def challenge_two(
    combine_geodata: bool = typer.Argument(
        True, help="Joins geojson data to the service request dataset."
    ),
    threshold: float = typer.Argument(
        0.0045,
        help="Threshold for joins. Smaller values will result in more precise joins.",
    ),
    check: bool = typer.Argument(True, help="Run checks for joined data."),
    thesholds_checks: bool = typer.Argument(
        False,
        help="Iterates through a set of thresholds and runs checks.\nThis may take a while to complete.",
    ),
):
    if combine_geodata:
        df = join_geodata(threshold=threshold)
        if check:
            run_checks(df)
    if thesholds_checks:
        test_thresholds()


@app.command()
def challenge_three(
    download_wind_data: bool = typer.Argument(
        True, help="Download wind file and write out to data/wind_data.ods"
    ),
    join: bool = typer.Argument(True, help="Joins wind data to subsample of a suburb"),
    suburb: str = typer.Argument(
        "BELLVILLE SOUTH",
        help="Suburb for which centroid is calculated to generate subsample of service data. Note: only BELLVILLE SOUTH wind data is currently available.",
    ),
    anonymise_data: bool = typer.Argument(True, help="Anonymise augmented data."),
    show: bool = True,
    save: bool = False,
):
    if download_wind_data:
        get_winds_data()
    if join:
        logging.info(f"Collecting service data augmented by winds data...")
        augmented_service_data = join_wind_to_service(suburb)
    if not (anonymise_data & join) & anonymise_data:
        logging.error("You MUST opt for --join if you want to anonymise data")
    if anonymise_data & join:
        augmented_service_data = anonymise(augmented_service_data)
    if show:
        logging.info(f"{augmented_service_data}")
    if save:
        augmented_service_data.to_csv("data/augmented_service_data.csv")

if __name__ == "__main__":
    app()
