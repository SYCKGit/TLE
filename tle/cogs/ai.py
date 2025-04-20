import google.generativeai as genai
from functools import partial
from discord.ext import commands
from os import environ

genai.configure(api_key=environ.get("GEMINI_KEY"))

# Set up the model
generation_config = {
    "temperature": 1,
    "top_p": 1,
    "top_k": 1,
    "max_output_tokens": 1000
}

safety_settings = [
    {
        "category": "HARM_CATEGORY_HARASSMENT",
        "threshold": "BLOCK_NONE"
    },
    {
        "category": "HARM_CATEGORY_HATE_SPEECH",
        "threshold": "BLOCK_NONE"
    },
    {
        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
        "threshold": "BLOCK_NONE"
    },
    {
        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
        "threshold": "BLOCK_NONE"
    }
]

class AICog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            generation_config=generation_config,
            safety_settings=safety_settings
        )
        self.convos = {0: self.model.start_chat(history=[])}

    async def break_reply(self, ctx, text):
        while text:
            await ctx.reply(text[:2000])
            text = text[2000:]

    @commands.command()
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def ask(self, ctx, *, prompt):
        response = await self.bot.loop.run_in_executor(None, partial(self.model.generate_content, prompt))
        try:
            text = response.text
        except ValueError:
            text = f"There was an error:```\n{response.prompt_feedback}\n```"
        await self.break_reply(ctx, text)

    async def cog_command_error(self, ctx, exc):
        if isinstance(exc, commands.CommandOnCooldown):
            return await ctx.send(str(exc))
        await ctx.send("There was an error :(")
        raise exc

    async def send_msg(self, ctx, id, prompt):
        await self.bot.loop.run_in_executor(None, partial(self.convos[id].send_message, prompt))
        await self.break_reply(ctx, self.convos[id].last.text)

    @commands.command()
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def chat(self, ctx, *, prompt):
        if ctx.author.id not in self.convos:
            self.convos[ctx.author.id] = self.model.start_chat(history=[])
        await self.send_msg(ctx, ctx.author.id, prompt)

    @commands.command(aliases=["gc"])
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def gchat(self, ctx, *, prompt):
        await self.send_msg(ctx, 0, prompt)

    @commands.command()
    async def fresh(self, ctx):
        if ctx.author.id in self.convos:
            del self.convos[ctx.author.id]
        await ctx.message.add_reaction("✅")

    @commands.command()
    async def gfresh(self, ctx):
        self.convos[0] = self.model.start_chat(history=[])
        await ctx.message.add_reaction("✅")

async def setup(bot: commands.Bot):
    await bot.add_cog(AICog(bot))
