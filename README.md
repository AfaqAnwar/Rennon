# Rennon

Rennon is a Deep Learning Chatbot "capable" of holding a conversation 
similar to that of a human.

## Current Status

Rennon V 1.0 is trained with a vocabulary size of 100,000. 

V 1.0 was trained on eight months of Reddit comments.

Specifically on  57,182,429 pairs of comments and replies.

```
Months filtered.

01/2017
02/2017
03/2017
12/2017
01/2018
10/2018
11/2018
12/2018
```
The deployable model is obtainable through the release pages of this repository.

To currently interact with Rennon you can download a model and run the inference.py script locally. This will allow you to see all of the replies with their respective scores.

_In the near future Rennon will be deployed through a Django Web Application._

### Notes

Developed using a modified version of Google's NMT translation model with an encoder and decoder.

This project was my final project for the AP Computer Science A class I took during my senior year at Brooklyn Technical High School.

