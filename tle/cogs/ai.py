import google.generativeai as genai
from functools import partial
from discord.ext import commands
from aiohttp import ClientSession

with open("api_key", "r") as f:
    key = f.read().strip()
genai.configure(api_key=key)

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

async def in_ag(ctx):
    return ctx.guild and ctx.guild.id == 1190034382560436274

class AICog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            generation_config=generation_config,
            safety_settings=safety_settings
        )
        self.sus_url = ""
        self.sus_model_name = "wizard-vicuna-uncensored:13b"
        self.api_retry = 3
        self.session = ClientSession()
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

    @commands.command()
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def sus(self, ctx, *, prompt):
        for _ in range(self.api_retry):
            try:
                resp = await self.session.post(f"https://{self.sus_url}/api/generate", json={"model": self.sus_model_name, "prompt": prompt, "stream": False, "options": dict(temperature=1)})
                text = (await resp.json())["response"]
                break
            except:
                continue
        else:
            return await ctx.reply("There was an error connecting to sus API 😔")
        await self.break_reply(ctx, text)

    @commands.command()
    @commands.check(in_ag)
    async def susurl(self, ctx, url):
        self.sus_url = url
        await ctx.message.add_reaction("✅")

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
