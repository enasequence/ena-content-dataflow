#!/usr/bin/env python3.7

import os, sys, argparse, re
from datetime import datetime

"""
This script will generate an updated XML file from the template XML, adding
the relevant <CHILD_PROJECT> fields. Note: the template XML *must* have a 
<UMBRELLA_PROJECT/> tag present.

Usage: add_to_umbrella_project.py --xml <template_xml> --file <list of projects to add>

Options:
    --xml       : (required) path to XML template file
    --projects  : (required) path to file containing one project accession per line
    --outdir    : (optional) name of output directory (default: umbrella_project_<timestamp>)

"""

parser = argparse.ArgumentParser(
    description="""This script will generate an updated XML file from the template XML, adding
    the relevant <CHILD_PROJECT> fields. Note: the template XML *must* have an
    <UMBRELLA_PROJECT/> tag present."""
)
parser.add_argument('--xml',      help="(required) path to XML template file")
parser.add_argument('--projects', help="(required) path to file containing one project accession per line")
parser.add_argument('--outdir',   help="(optional) name of output directory (default: umbrella_project_<timestamp>)");
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
umbrella_w_children_xml = []
with open(opts.xml) as xml_file:
    for line in xml_file:
        line = line.rstrip()
        # print(line)
        umbrella_w_children_xml.append(line)
        regex = re.match("(\s+)<UMBRELLA_PROJECT/>", line)
        if regex:
            umbrella = True
            indent = regex.group(1)
            child_project_xml = [f"{indent}{x}" for x in child_project_xml]
            # print("\n".join(child_project_xml))
            umbrella_w_children_xml.append("\n".join(child_project_xml))

if not umbrella:
    sys.stderr.write("\n\nError: <UMBRELLA_PROJECT/> tag not found in the --xml file : no <CHILD_PROJECT> tags added\n\n")

# generate and create the output directory
if opts.outdir:
    outdir = opts.outdir
else:
    now = datetime.now()
    now_str = now.strftime("%d%m%y_%H%M%S")
    outdir = f"umbrella_project_{now_str}"
print(f"Creating dir '{outdir}'")
os.mkdir(outdir)

# write the umbrella xml with child projects to file
with open(f"{outdir}/umbrella_with_children.xml", 'w') as ufile:
    ufile.write("\n".join(umbrella_w_children_xml))

# write the submission xml to file
submission_xml = """
<SUBMISSION>
     <ACTIONS>
         <ACTION>
             <MODIFY/>
         </ACTION>
    </ACTIONS>
</SUBMISSION>
"""
with open(f"{outdir}/submission.xml", 'w') as sfile:
    sfile.write(submission_xml)


# create and print the curl command required to submit the updated objects
print(f"curl -u User:Password -F \"SUBMISSION=@{outdir}/submission.xml\" -F \"PROJECT=@{outdir}/umbrella_with_children.xml\" \"https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/\"")
