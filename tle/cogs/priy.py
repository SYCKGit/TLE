import markovify
import spacy
import os
from discord.ext import commands

nlp = spacy.load("en_core_web_sm")

class POSifiedText(markovify.NewlineText):
    def word_split(self, sentence: str):
        return ["::".join((word.orth_, word.pos_)) for word in nlp(sentence)]

    def word_join(self, words: list[str]):
        sentence = " ".join(word.split("::")[0] for word in words)
        return sentence

class PriyCog(commands.Cog):
    def __init__(self):
        if not os.path.exists("priy.json"):
            with open("history.txt", "r") as f:
                history = f.read()
            self.model = POSifiedText(history, well_formed=False)
            with open("priy.json", "w") as f:
                f.write(self.model.to_json())
        else:
            with open("priy.json", "r") as f:
                self.model = POSifiedText.from_json(f.read())

    def cog_check(self, ctx: commands.Context) -> bool:
        return bool(ctx.guild and ctx.guild.id == 1180191733431144458)

    @commands.command()
    async def priy(self, ctx: commands.Context):
        async with ctx.channel.typing():
            messages = "\n".join([message.content async for message in ctx.channel.history(limit=50) if message.content and not message.content.lower().startswith(".priy")])
            recent_model = POSifiedText(messages, well_formed=False)
            model = markovify.combine([self.model, recent_model], [1, 20])
            final = ""
            while not final:
                final = model.make_sentence()
            await ctx.send(final)

async def setup(bot: commands.Bot):
    if os.path.exists("priy.json") or os.path.exists("history.txt"):
        await bot.add_cog(PriyCog())