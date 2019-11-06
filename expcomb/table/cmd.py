import click
from io import StringIO
from subprocess import call
from pylatex import Document, NoEscape, Package

from expcomb.doc_utils import pk
from expcomb.filter import empty_filter
from .utils import clds_from_dbs, docs_from_dbs, highlights_from_dbs, key_doc_selectors


def indicate_highlights(docs, highlights, pk_extra, key):
    highlights_keyed = set(key_doc_selectors(highlights))
    for doc in docs:
        doc[key] = pk(doc, pk_extra) in highlights_keyed


def add_clds(docs, clds, pk_extra):
    for doc in docs:
        key = pk(doc, pk_extra)
        if key in clds:
            doc["clds"] = clds[key]


def add_tables(group, tables_tpls, pk_extra):

    @group.command("tables")
    @click.pass_context
    @click.argument("db_paths", type=click.Path(), nargs=-1)
    @click.option("--preview/--no-preview")
    @click.option("--table", "-t", multiple=True)
    def tables_cmd(ctx, db_paths, preview, table):
        if preview:
            latex_doc = Document(
                geometry_options={"paperwidth": "100cm", "paperheight": "100cm"}
            )
            latex_doc.packages.append(Package("tabu"))
            latex_doc.packages.append(Package("booktabs"))
            latex_doc.packages.append(Package("multirow"))
            latex_doc.packages.append(Package("xcolor"))
            latex_doc.packages.append(Package("colortbl"))

        for table_tpl in tables_tpls:
            name, spec = table_tpl[:2]
            if table and name not in table:
                continue
            if len(table_tpl) > 2:
                filter = table_tpl[2]
            else:
                filter = empty_filter
            docs = docs_from_dbs(db_paths, filter, pk_extra)
            highlights = highlights_from_dbs(db_paths, filter, "guesses")
            maxs = highlights_from_dbs(db_paths, filter, "max")
            clds = clds_from_dbs(db_paths, filter)
            add_clds(docs, clds, pk_extra)
            indicate_highlights(docs, highlights, pk_extra, "highlight")
            indicate_highlights(docs, maxs, pk_extra, "max")

            table_code = StringIO()
            table_code.write("\n% Table: {}\n".format(name))
            spec.print(docs, outf=table_code)
            if preview:
                latex_doc.append(NoEscape(table_code.getvalue()))
                latex_doc.append(NoEscape("\\clearpage"))
            print(table_code.getvalue())

        if preview:
            latex_doc.generate_pdf()
            call(["evince", "default_filepath.pdf"])
