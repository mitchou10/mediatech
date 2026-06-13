from justices.base import BaseJustice, parse_args


class ConseilDetat(BaseJustice):
    filename_xml_col = "Nom du fichier .xml"
    filename_zip_col = "Nom du fichier .zip"
    date_col = "Date de lecture"


if __name__ == "__main__":
    args = parse_args()

    obj = ConseilDetat(
        config_loader={
            "download_url": "https://opendata.justice-administrative.fr/DCE/CE_documents_reverses.csv"
        },
        folder_download="data/unprocessed/jurisprudence",
        base_url="https://opendata.justice-administrative.fr/DCE/YEAR/MONTH/CE_YEARMONTH.zip",
    )
    obj.run(repo_id=f"{args.user_id}/conseil-detat-full-documents")
