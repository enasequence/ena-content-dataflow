#!/usr/bin/env python3.7
import sys, argparse, re

"""
This script will generate an updated XML file from the template XML, adding
the relevant <CHILD_PROJECT> fields. Note: the template XML *must* have a 
<UMBRELLA_PROJECT/> tag present.

Usage: python3 add_to_umbrella_project.py --xml <template_xml> --file <list of projects to add>

Options:
    --xml       : (required) path to XML template file
    --projects  : (required) path to file containing one project accession per line

"""

parser = argparse.ArgumentParser(
    description="""This script will generate an updated XML file from the template XML, adding
    the relevant <CHILD_PROJECT> fields. Note: the template XML *must* have an
    <UMBRELLA_PROJECT/> tag present."""
)
parser.add_argument('--xml',      help="path to XML template file")
parser.add_argument('--projects', help="path to file containing one project accession per line")
opts = parser.parse_args(sys.argv[1:])

if ( not ( opts.xml and opts.projects ) ):
    sys.stderr.write("--xml and --projects are required\n\n")
    sys.exit(1)

# construct the child projects XML
child_project_xml = ['<RELATED_PROJECTS>']
with open(opts.projects) as projects_file:
    for line in projects_file:
        project_acc = line.rstrip()
        child_project_xml.append(f"\t<RELATED_PROJECT><CHILD_PROJECT accession=\"{project_acc}\"/></RELATED_PROJECT>")
child_project_xml.append('</RELATED_PROJECTS>')

# parse the template XML and place the child projects in the correct position
umbrella = False
with open(opts.xml) as xml_file:
    for line in xml_file:
        line = line.rstrip()
        print(line)
        regex = re.match("(\s+)<UMBRELLA_PROJECT/>", line)
        if regex:
            umbrella = True
            indent = regex.group(1)
            child_project_xml = [f"{indent}{x}" for x in child_project_xml]
            print("\n".join(child_project_xml))

if not umbrella:
    sys.stderr.write("\n\nError: <UMBRELLA_PROJECT/> tag not found in the --xml file : no <CHILD_PROJECT> tags added\n\n")
