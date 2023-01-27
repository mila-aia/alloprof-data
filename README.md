# Alloprof dataset

This is the dataset refered to in our paper:

ADD_PAPER

ADD_ARXIV_LINK

This dataset was provided by [AlloProf](https://www.alloprof.qc.ca/), an organisation in Quebec, Canada offering resources and a help forum curated by a large number of teachers to students on all subjects taught from in primary and secondary school.

Raw data on questions is available in the following files:

- `data/questions/categories.json`: subjects and their corresponding id
- `data/questions/comments.json`: explanation (answer) data
- `data/questions/discussions.json`: question data
- `data/questions/grades.json`: grades and their corresponding id
- `data/questions/roles.json`: information about the user type for each user id

Raw data on reference pages is available in the following files:

- `data/pages/page-content-en.json`: data for the reference pages in English
- `data/pages/page-content-fr.json`: data for the reference pages in French

The data was parsed and structured using the script `scripts/parse_data.py` to create the file `data/alloprof.csv` with the following columns:

- `id` (str) : Id of the document
- `url` (str) : URL of the document
- `text` (str) : Parsed text of the document
- `language` (str) : Either "fr" or "en", the language of the document
- `user` (int) : Id corresponding to the user who asked the question
- `images` (str) : ";" separated list of URLs of images contained in the document
- `relevant` (str) : ";" separated list of document ids appearing as links in the explanation to that document. For files, this will always be empty as there are no corresponding explanation
- `is_query` (bool) : If this document is a question
- `subject` (str) : ";" separated list of school subjects the document is related to
- `grade` (str) : ";" separated list of school grade levels the document is related to
- `possible` (str) : ";" separated list of possible documents ids this document may refer to. This list corresponds to every document of the same subject and grade. For files, this will always be empty to speed up reading and writing

The `possible` column depends on arguments passed to the scripts to add related subjects, and lower and higher grade levels to the possible documents (see paper).

For images, a script to download them is available as `scripts/download_images.py`.

If you have any questions, don't hesitate to mail us at antoine.lefebvre-brossard@mila.quebec.

**Please cite our work as:**

```
ADD_BIBTEX
```
